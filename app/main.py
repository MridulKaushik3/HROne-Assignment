"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB   (real environment variables win over a local .env)

Layout of this file
  1. configuration, database handle, indexes
  2. time / rounding helpers and the business rules R1-R5
  3. request models and (de)serialisation
  4. employees, health
  5. attendance (punch-in, punch-out, list, regularize)
  6. analytics pipelines (builders are shared by the endpoints and /admin/explain)
  7. explain endpoint
"""
import json
import logging
import os
import re
import time
from calendar import monthrange
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

load_dotenv()  # override=False: variables already in the environment take priority
log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, database, indexes
# --------------------------------------------------------------------------- #
# tz_aware=True: PyMongo hands back UTC-aware datetimes instead of naive ones, so
# arithmetic and epoch conversion never depend on the server's local time zone.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    serverSelectionTimeoutMS=4000,
    connect=False,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: creating an index that already exists with the same spec is a no-op."""
    global _indexes_ready
    emp, logs = db.employees, db.attendance_logs
    # emp_code uniqueness is a DB guarantee (concurrent creates -> exactly one wins);
    # it also serves lookups by emp_code and the unfiltered employee list sort.
    emp.create_index([("emp_code", ASCENDING)], unique=True, name="emp_code_unique")
    # filter by department, sorted by emp_code (list) + department trend / explain.
    emp.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="dept_emp_code")
    # headcount: department + joined_on <= end of period (R9); also serves the no-department case.
    emp.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="dept_joined_on")
    emp.create_index([("joined_on", ASCENDING)], name="joined_on")
    # "at most one record per (emp_code, date)": makes punch-in race-safe, addresses PATCH,
    # and serves monthly / per-employee range scans.
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="emp_date_unique")
    # global list order (date desc, emp_code asc); month-wide analytics ($match on date range).
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp")
    # punch-out: "most recent record of this employee with punch_in <= t".
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_punch_in")
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        ensure_indexes()
    except PyMongoError as exc:  # DB not up yet: stay alive, /health reports 503 and retries
        log.warning("could not create indexes at startup: %s", exc)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database error"})


# --------------------------------------------------------------------------- #
# 2. Time, rounding and the business rules
# --------------------------------------------------------------------------- #
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]  # R6
LATE_GRACE_SECONDS = 10 * 60  # R2
MIN_OVERTIME_MINUTES = 30  # R3
MAX_SHIFT_SECONDS = 24 * 3600
MS_MIN, MS_MAX = 100_000_000_000, 4_102_444_800_000

# strict=True: floats, strings and booleans are rejected (422), not coerced.
EpochMs = Annotated[int, Field(strict=True, ge=MS_MIN, le=MS_MAX)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

HM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def ms_to_dt(ms: int) -> datetime:
    return EPOCH + timedelta(milliseconds=ms)  # integer arithmetic, no float drift


def dt_to_ms(value: datetime) -> int:
    if value.tzinfo is None:  # defensive: naive values coming from the DB are UTC
        value = value.replace(tzinfo=UTC)
    return (value - EPOCH) // timedelta(milliseconds=1)


def as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def trunc_dt(value: datetime) -> datetime:
    """R1: instants are truncated to whole seconds before any calculation."""
    return as_utc(value).replace(microsecond=0)


def ms_to_trunc_dt(ms: int) -> datetime:
    return ms_to_dt(ms - ms % 1000)


def now_dt() -> datetime:
    return ms_to_dt(time.time_ns() // 1_000_000)


def round_half_up(value, places: int = 2) -> float:
    """R4 / R8: half-up (not banker's, not binary-float rounding)."""
    d = value if isinstance(value, Decimal) else Decimal(repr(float(value)))
    return float(d.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def unprocessable(loc: tuple, msg: str):
    """Raise a 422 in FastAPI's standard validation-error shape."""
    raise RequestValidationError([{"type": "value_error", "loc": loc, "msg": msg, "input": None}])


def parse_date(value: str, loc: tuple) -> Date:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value or ""):
        unprocessable(loc, "must be a date formatted YYYY-MM-DD")
    try:
        return Date.fromisoformat(value)
    except ValueError:
        unprocessable(loc, "not a valid calendar date")


def parse_month(value: str, loc: tuple = ("query", "month")) -> tuple[Date, Date]:
    if not re.fullmatch(MONTH_RE, value or ""):
        unprocessable(loc, "must be a month formatted YYYY-MM")
    year, month = int(value[:4]), int(value[5:])
    if year < 1:
        unprocessable(loc, "year out of range")
    return Date(year, month, 1), Date(year, month, monthrange(year, month)[1])


def hm_minutes(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def is_overnight(emp: dict) -> bool:
    return hm_minutes(emp["shift_end"]) <= hm_minutes(emp["shift_start"])


def at_ist(day: Date, minutes: int) -> datetime:
    return datetime(day.year, day.month, day.day, minutes // 60, minutes % 60, tzinfo=IST)


def attendance_date(punch_in: datetime, emp: dict) -> Date:
    """R1: IST calendar date of the punch-in; for overnight shifts a punch-in earlier than
    shift_end belongs to the shift that started the previous day."""
    local = trunc_dt(punch_in).astimezone(IST)
    day = local.date()
    if is_overnight(emp):
        seconds_into_day = local.hour * 3600 + local.minute * 60 + local.second
        if seconds_into_day < hm_minutes(emp["shift_end"]) * 60:
            day -= timedelta(days=1)
    return day


def shift_start_at(day: Date, emp: dict) -> datetime:
    return at_ist(day, hm_minutes(emp["shift_start"]))


def shift_end_at(day: Date, emp: dict) -> datetime:
    """The end of an overnight shift falls on the next calendar day."""
    end_day = day + timedelta(days=1) if is_overnight(emp) else day
    return at_ist(end_day, hm_minutes(emp["shift_end"]))


def calc_late_minutes(punch_in: datetime, day: Date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift_start; then floor-minutes since shift_start."""
    seconds = int((trunc_dt(punch_in) - shift_start_at(day, emp)).total_seconds())
    return seconds // 60 if seconds > LATE_GRACE_SECONDS else 0


def calc_overtime_minutes(punch_out: datetime, day: Date, emp: dict) -> int:
    """R3: floor-minutes from shift_end to punch_out, counted only if >= 30."""
    seconds = int((trunc_dt(punch_out) - shift_end_at(day, emp)).total_seconds())
    minutes = seconds // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def calc_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals, half-up."""
    seconds = int((trunc_dt(punch_out) - trunc_dt(punch_in)).total_seconds())
    return round_half_up(Decimal(seconds) / Decimal(3600), 2)


def compute_derived(status: str, punch_in, punch_out, day: Date, emp: dict) -> dict:
    """The four derived fields for a record in its final state (DATA_MODEL.md, section 3)."""
    derived = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return derived
    derived["late_minutes"] = calc_late_minutes(punch_in, day, emp)
    if punch_out is not None:
        hours = calc_work_hours(punch_in, punch_out)
        derived["work_hours"] = hours
        derived["overtime_minutes"] = calc_overtime_minutes(punch_out, day, emp)
        derived["half_day"] = hours < 4.5  # R5, on the rounded value
    return derived


def count_weekdays(first: Date, last: Date) -> int:
    """Mon-Fri days in [first, last] (R7)."""
    if last < first:
        return 0
    full_weeks, rest = divmod((last - first).days + 1, 7)
    total = full_weeks * 5
    start = first + timedelta(days=full_weeks * 7)
    return total + sum(1 for i in range(rest) if (start + timedelta(days=i)).weekday() < 5)


# --------------------------------------------------------------------------- #
# 3. Models and serialisation
# --------------------------------------------------------------------------- #
class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field("09:30", pattern=HM_RE)
    shift_end: str = Field("18:30", pattern=HM_RE)
    joined_on: str

    @field_validator("name", "department")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("must not be blank")
        return v

    @field_validator("joined_on")
    @classmethod
    def valid_date(cls, v: str) -> str:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", v):
            raise ValueError("must be YYYY-MM-DD")
        Date.fromisoformat(v)  # ValueError -> 422
        return v

    @model_validator(mode="after")
    def shifts_differ(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def no_explicit_nulls(self):
        # omitted = unchanged; an explicit null is not a valid value for these fields
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} must not be null")
        return self


def _ms_or_none(value):
    return dt_to_ms(value) if isinstance(value, datetime) else value


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"],
        "joined_on": doc["joined_on"],
        "created_at": _ms_or_none(doc.get("created_at")),
    }


def record_out(doc: dict) -> dict:
    """Stored document -> API shape: no _id, instants as epoch ms, legacy gaps filled in."""
    history = []
    for entry in doc.get("history") or []:
        changes = {
            key: {"from": _ms_or_none(v.get("from")), "to": _ms_or_none(v.get("to"))}
            for key, v in (entry.get("changes") or {}).items()
        }
        history.append({"at": _ms_or_none(entry.get("at")), "by": entry.get("by"),
                        "reason": entry.get("reason"), "changes": changes})
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": _ms_or_none(doc.get("punch_in")),
        "punch_out": _ms_or_none(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": history,
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Health and employees
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    doc["created_at"] = now_dt()
    try:
        db.employees.insert_one(doc)  # unique index decides the race, not a prior find
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    q = {"department": department} if department is not None else {}
    total = db.employees.count_documents(q)
    cursor = db.employees.find(q).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


# --------------------------------------------------------------------------- #
# 5. Attendance
# --------------------------------------------------------------------------- #
@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_trunc_dt(body.punched_at) if body.punched_at is not None else trunc_dt(now_dt())
    day = attendance_date(ts, emp)
    doc = {
        "emp_code": body.emp_code,
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": calc_late_minutes(ts, day, emp),
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }
    try:
        db.attendance_logs.insert_one(doc)  # unique (emp_code, date): exactly one racer wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_trunc_dt(body.punched_at) if body.punched_at is not None else trunc_dt(now_dt())
    # Most recent record whose punch_in <= punched_at. Records without punch_in (ABSENT/LEAVE)
    # never match a $lte on a date, so they are skipped automatically.
    rec = db.attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}}, sort=[("punch_in", DESCENDING)]
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    p_in = trunc_dt(rec["punch_in"])
    if ts <= p_in:
        unprocessable(("body", "punched_at"), "must be after the punch-in")
    if (ts - p_in).total_seconds() > MAX_SHIFT_SECONDS:
        unprocessable(("body", "punched_at"), "more than 24 hours after the punch-in")

    day = Date.fromisoformat(rec["date"])
    derived = compute_derived(rec["status"], p_in, ts, day, emp)
    # The filter on punch_out makes the update a compare-and-set: of two simultaneous
    # punch-outs exactly one matches; the other gets None back and answers 409.
    updated = db.attendance_logs.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {"$set": {"punch_out": ts, **{k: v for k, v in derived.items() if k != "late_minutes"}}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


def build_attendance_query(emp_code, date_from, date_to, status, page, page_size) -> dict:
    """Shared by GET /attendance and /admin/explain/attendance_list."""
    lo = parse_date(date_from, ("query", "date_from")) if date_from is not None else None
    hi = parse_date(date_to, ("query", "date_to")) if date_to is not None else None
    if lo and hi and lo > hi:
        unprocessable(("query", "date_from"), "date_from must not be after date_to")
    q: dict[str, Any] = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    if lo or hi:
        q["date"] = {}
        if lo:
            q["date"]["$gte"] = lo.isoformat()
        if hi:
            q["date"]["$lte"] = hi.isoformat()
    if status is not None:
        q["status"] = status
    return {
        "filter": q,
        "sort": {"date": -1, "emp_code": 1},
        "skip": (page - 1) * page_size,
        "limit": page_size,
        "projection": {"_id": 0},
    }


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    spec = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
    col = db.attendance_logs
    total = col.count_documents(spec["filter"]) if spec["filter"] else col.estimated_document_count()
    cursor = (
        col.find(spec["filter"], spec["projection"])
        .sort(list(spec["sort"].items()))
        .skip(spec["skip"])
        .limit(spec["limit"])
    )
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


REGULARIZE_RETRIES = 5
TRACKED_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


def _same(field: str, old, new) -> bool:
    if field in ("punch_in", "punch_out"):
        if old is None or new is None:
            return old is None and new is None
        return dt_to_ms(trunc_dt(old)) == dt_to_ms(trunc_dt(new))
    return old == new


def _resolve_instant(supplied_ms: Optional[int], stored: Optional[datetime]) -> Optional[datetime]:
    """Value after a correction: the stored one if nothing (or the same second) was supplied."""
    if supplied_ms is None:
        return stored
    wanted = ms_to_trunc_dt(supplied_ms)
    if stored is not None and trunc_dt(stored) == wanted:
        return stored
    return wanted


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeIn):
    day = parse_date(date, ("path", "date"))
    emp = get_employee_or_404(emp_code)

    for _ in range(REGULARIZE_RETRIES):
        rec = db.attendance_logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")

        old = {
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "work_hours": rec.get("work_hours"),
            "late_minutes": rec.get("late_minutes") or 0,
            "overtime_minutes": rec.get("overtime_minutes") or 0,
            "half_day": bool(rec.get("half_day", False)),
        }
        new_status = body.status or old["status"]

        if new_status in ("ABSENT", "LEAVE"):
            if body.punch_in is not None or body.punch_out is not None:
                unprocessable(("body", "status"), f"{new_status} clears punch times; do not supply them")
            new_in = new_out = None
        else:
            new_in = _resolve_instant(body.punch_in, old["punch_in"])
            new_out = _resolve_instant(body.punch_out, old["punch_out"])
            if new_in is None:
                unprocessable(("body", "punch_in"), "a presence status requires a punch_in")

        if _same("status", old["status"], new_status) and _same("punch_in", old["punch_in"], new_in) \
                and _same("punch_out", old["punch_out"], new_out):
            unprocessable(("body",), "the request changes nothing")

        if new_status in PRESENCE:
            if body.punch_in is not None and attendance_date(new_in, emp) != day:
                unprocessable(("body", "punch_in"), "punch_in must stay on the record's attendance date")
            if new_out is not None and (body.punch_in is not None or body.punch_out is not None):
                gap = (trunc_dt(new_out) - trunc_dt(new_in)).total_seconds()
                if gap <= 0:
                    unprocessable(("body", "punch_out"), "punch_out must be after punch_in")
                if gap > MAX_SHIFT_SECONDS:
                    unprocessable(("body", "punch_out"), "punch_out must be within 24 hours of punch_in")

        derived = compute_derived(new_status, new_in, new_out, day, emp)
        final = {"status": new_status, "punch_in": new_in, "punch_out": new_out, **derived}
        changes = {
            f: {"from": old[f], "to": final[f]} for f in TRACKED_FIELDS if not _same(f, old[f], final[f])
        }
        entry = {"at": now_dt(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

        # Optimistic concurrency: only write if the record is still exactly as we read it.
        # history only ever grows, so its length acts as a version number.
        n = len(rec.get("history") or [])
        version = {"$or": [{"history": None}, {"history": {"$size": 0}}]} if n == 0 else {"history": {"$size": n}}
        guard = {
            "_id": rec["_id"], "status": rec["status"],
            "punch_in": rec.get("punch_in"), "punch_out": rec.get("punch_out"),
            **version,
        }
        updated = db.attendance_logs.find_one_and_update(
            guard, {"$set": final, "$push": {"history": entry}}, return_document=ReturnDocument.AFTER
        )
        if updated is not None:
            return record_out(updated)
        # someone else changed the record between our read and write: re-read and redo

    raise HTTPException(409, "record was modified concurrently, please retry")


# --------------------------------------------------------------------------- #
# 6. Analytics (every number is computed by MongoDB; Python only shapes / rounds results)
# --------------------------------------------------------------------------- #
def _num(field: str) -> dict:
    return {"$ifNull": [field, 0]}


def _is_present(status: str) -> dict:
    return {"$in": [{"$ifNull": [status, ""]}, PRESENCE]}


def _half_aware(status: str, half_day: str) -> dict:
    """1 for a present record, 0.5 for a present half day, 0 otherwise (R5, R6)."""
    return {"$cond": [_is_present(status),
                      {"$cond": [{"$eq": [{"$ifNull": [half_day, False]}, True]}, 0.5, 1]}, 0]}


def _dow(date_expr) -> dict:
    # $dayOfWeek: 1 = Sunday ... 7 = Saturday. "date" is a plain calendar string, parsed as UTC.
    return {"$dayOfWeek": {"$dateFromString": {"dateString": date_expr, "format": "%Y-%m-%d",
                                               "onError": None, "onNull": None}}}


def _is_weekday(date_expr) -> dict:
    return {"$and": [{"$gte": [_dow(date_expr), 2]}, {"$lte": [_dow(date_expr), 6]}]}


def _present_weekday_value(prefix: str = "$") -> dict:
    """present-day value of one record, counted only on Mon-Fri (R7)."""
    return {"$cond": [_is_weekday(f"{prefix}date"),
                      _half_aware(f"{prefix}status", f"{prefix}half_day"), 0]}


def _is_late(prefix: str = "$") -> dict:
    return {"$cond": [{"$gt": [_num(f"{prefix}late_minutes"), 0]}, 1, 0]}


# ---- employee monthly ------------------------------------------------------ #
def build_employee_monthly_pipeline(emp_code: str, month: str) -> list:
    first, last = parse_month(month)
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": _present_weekday_value()},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": _is_late()},
            "total_late_minutes": {"$sum": _num("$late_minutes")},
            "total_overtime_minutes": {"$sum": _num("$overtime_minutes")},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(..., pattern=MONTH_RE)):
    first, last = parse_month(month)
    emp = get_employee_or_404(emp_code)
    row = next(db.attendance_logs.aggregate(build_employee_monthly_pipeline(emp_code, month)), None) or {}
    joined = Date.fromisoformat(emp["joined_on"])
    working_days = count_weekdays(max(first, joined), last)
    present = float(row.get("present_days", 0))
    pct = None
    if working_days:
        pct = round_half_up(Decimal(repr(present)) * 100 / Decimal(working_days), 2)
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": round_half_up(present, 2),
        "leave_days": int(row.get("leave_days", 0)),
        "late_count": int(row.get("late_count", 0)),
        "total_late_minutes": int(row.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(row.get("total_overtime_minutes", 0)),
        "attendance_pct": pct,
    }


# ---- department summary ---------------------------------------------------- #
def build_department_summary_pipeline(month: str, department: Optional[str]) -> list:
    """Starts from employees (so people with zero logs still count, R9) and joins their
    month of logs through the (emp_code, date) index."""
    first, last = parse_month(month)
    match: dict[str, Any] = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    p = "$logs."
    has_hours = {"$and": [_is_present(f"{p}status"), {"$ne": [{"$ifNull": [f"{p}work_hours", None]}, None]}]}
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
            "pipeline": [
                {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
                {"$project": {"_id": 0, "date": 1, "status": 1, "late_minutes": 1, "work_hours": 1, "half_day": 1}},
            ],
            "as": "logs",
        }},
        {"$unwind": {"path": "$logs", "preserveNullAndEmptyArrays": True}},
        # level 1: one row per employee (keeps headcount exact, however many logs they have)
        {"$group": {
            "_id": "$emp_code",
            "department": {"$first": "$department"},
            "present_days": {"$sum": _present_weekday_value(p)},
            "wh_sum": {"$sum": {"$cond": [has_hours, f"{p}work_hours", 0]}},
            "wh_cnt": {"$sum": {"$cond": [has_hours, 1, 0]}},
            "late_count": {"$sum": _is_late(p)},
            "total_late_minutes": {"$sum": {"$cond": [{"$gt": [_num(f"{p}late_minutes"), 0]}, f"{p}late_minutes", 0]}},
            "leave_count": {"$sum": {"$cond": [{"$eq": [f"{p}status", "LEAVE"]}, 1, 0]}},
            "on_duty_count": {"$sum": {"$cond": [{"$eq": [f"{p}status", "ON_DUTY"]}, 1, 0]}},
        }},
        # level 2: one row per department; avg_work_hours later = sum / count (records, not employees)
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$present_days"},
            "wh_sum": {"$sum": "$wh_sum"},
            "wh_cnt": {"$sum": "$wh_cnt"},
            "late_count": {"$sum": "$late_count"},
            "total_late_minutes": {"$sum": "$total_late_minutes"},
            "leave_count": {"$sum": "$leave_count"},
            "on_duty_count": {"$sum": "$on_duty_count"},
        }},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(..., pattern=MONTH_RE), department: Optional[str] = None):
    pipeline = build_department_summary_pipeline(month, department)
    items = []
    for r in db.employees.aggregate(pipeline):
        count = int(r["wh_cnt"])
        avg = None
        if count:
            total = Decimal(repr(round(float(r["wh_sum"]), 6)))
            avg = round_half_up(total / count, 2)
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": round_half_up(float(r["present_days"]), 2),
            "avg_work_hours": avg,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["total_late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


# ---- late leaderboard ------------------------------------------------------ #
def build_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> list:
    first, last = parse_month(month)
    p: list[dict] = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees are dropped here
    ]
    if department is not None:
        p.append({"$match": {"emp.department": department}})  # rank inside the department only
    p += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        # limit is applied to the RANK (after ranking), so ties at the cutoff are all kept.
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return p


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_RE),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    rows = list(db.attendance_logs.aggregate(build_leaderboard_pipeline(month, limit, department)))
    items = [{
        "rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
        "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"]),
    } for r in rows]
    return {"month": month, "items": items}


# ---- department trend ------------------------------------------------------ #
MAX_TREND_DAYS = 92


def prepare_trend(department: str, date_from: Optional[str], date_to: Optional[str]):
    """Validation + the department's roster. Returns what build_trend_pipeline needs."""
    if date_from is None:
        unprocessable(("query", "from"), "field required")
    if date_to is None:
        unprocessable(("query", "to"), "field required")
    frm = parse_date(date_from, ("query", "from"))
    to = parse_date(date_to, ("query", "to"))
    if to < frm:
        unprocessable(("query", "to"), "to must not be before from")
    if (to - frm).days + 1 > MAX_TREND_DAYS:
        unprocessable(("query", "to"), f"range must not exceed {MAX_TREND_DAYS} days")
    roster = list(db.employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not roster:
        raise HTTPException(404, "unknown department")
    return frm, to, [r["emp_code"] for r in roster], sorted(r["joined_on"] for r in roster)


def build_trend_pipeline(department: str, frm: Date, to: Date, codes: list, joins: list) -> list:
    n_days = (to - frm).days + 1
    start = datetime(frm.year, frm.month, frm.day)  # naive datetimes are encoded as UTC by PyMongo
    # One zero-valued row per calendar day, generated inside the database ($range), so days
    # with no records (and weekends) still appear. It is merged with the real per-day sums below.
    filler = [
        {"$match": {"department": department}},
        {"$limit": 1},
        {"$project": {"_id": 0, "offset": {"$range": [0, n_days]}}},
        {"$unwind": "$offset"},
        {"$project": {
            "_id": {"$dateToString": {"date": {"$dateAdd": {"startDate": start, "unit": "day", "amount": "$offset"}},
                                      "format": "%Y-%m-%d"}},
            "present": {"$literal": 0}, "late": {"$literal": 0},
        }},
    ]
    w = {"documents": [-6, 0]}  # current row + up to 6 preceding rows, inside the requested range only
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": frm.isoformat(), "$lte": to.isoformat()}}},
        {"$group": {"_id": "$date", "present": {"$sum": _half_aware("$status", "$half_day")},
                    "late": {"$sum": _is_late()}}},
        {"$unionWith": {"coll": "employees", "pipeline": filler}},
        {"$group": {"_id": "$_id", "present": {"$sum": "$present"}, "late": {"$sum": "$late"}}},
        {"$addFields": {
            "is_working_day": _is_weekday("$_id"),
            # R9: employees of the department who had joined on or before that day
            "headcount": {"$size": {"$filter": {"input": {"$literal": joins}, "as": "j",
                                                "cond": {"$lte": ["$$j", "$_id"]}}}},
        }},
        # Rates are held as integers in units of 1/10000 so half-up rounding is exact:
        #   k = floor(present/headcount * 10000 + 1/2) = floor((present*20000 + headcount) / (2*headcount))
        {"$addFields": {"_k": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$floor": {"$divide": [{"$add": [{"$multiply": ["$present", 20000]}, "$headcount"]},
                                    {"$multiply": [2, "$headcount"]}]}},
            None]}}},
        {"$addFields": {"_ind": {"$cond": [{"$eq": ["$_k", None]}, 0, 1]}}},
        {"$setWindowFields": {"sortBy": {"_id": 1}, "output": {
            "_sum": {"$sum": "$_k", "window": w},
            "_cnt": {"$sum": "$_ind", "window": w},
        }}},
        {"$project": {
            "_id": 0, "date": "$_id", "is_working_day": 1, "headcount": 1,
            "present_count": "$present", "late_count": "$late",
            "attendance_rate": {"$cond": [{"$eq": ["$_k", None]}, None, {"$divide": ["$_k", 10000]}]},
            "moving_avg_7d": {"$cond": [
                {"$eq": ["$_cnt", 0]}, None,
                {"$divide": [{"$floor": {"$divide": [{"$add": [{"$multiply": ["$_sum", 2]}, "$_cnt"]},
                                                      {"$multiply": [2, "$_cnt"]}]}}, 10000]}]},
        }},
        {"$sort": {"date": 1}},
    ]


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None):
    frm, to_d, codes, joins = prepare_trend(department, from_, to)
    items = list(db.attendance_logs.aggregate(build_trend_pipeline(department, frm, to_d, codes, joins)))
    for row in items:
        row["headcount"] = int(row["headcount"])
        row["late_count"] = int(row["late_count"])
        row["present_count"] = float(row["present_count"])
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. Explain
# --------------------------------------------------------------------------- #
def _require(name: str, value):
    if value is None:
        unprocessable(("query", name), "field required for this endpoint")
    return value


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"],
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_RE),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    # Each branch calls the same builder the real endpoint calls, so the explained
    # operation is the one that really runs.
    if endpoint == "attendance_list":
        spec = build_attendance_query(emp_code, date_from, date_to, status, page, page_size)
        collection = "attendance_logs"
        command = {"find": collection, **spec}
    else:
        if endpoint == "employee_monthly":
            collection, pipeline = "attendance_logs", build_employee_monthly_pipeline(_require("emp_code", emp_code), _require("month", month))
        elif endpoint == "department_summary":
            collection, pipeline = "employees", build_department_summary_pipeline(_require("month", month), department)
        elif endpoint == "late_leaderboard":
            collection, pipeline = "attendance_logs", build_leaderboard_pipeline(_require("month", month), limit, department)
        else:
            dept = _require("department", department)
            frm, to_d, codes, joins = prepare_trend(dept, from_, to)
            collection, pipeline = "attendance_logs", build_trend_pipeline(dept, frm, to_d, codes, joins)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}
    raw = db.command({"explain": command, "verbosity": "executionStats"})
    return {
        "endpoint": endpoint,
        "collection": collection,
        "explain": json.loads(json_util.dumps(raw, json_options=json_util.RELAXED_JSON_OPTIONS)),
    }

