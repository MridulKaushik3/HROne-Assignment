# Employee Attendance & Analytics API
Hi I am Mridul. This is not me best work but I can do best in your proper guidance and time. With few try and my experties in python programming, prompt writing and backend developement I tried and here is the submission. It is not complete bcoz I am not genius of fast API but if you give me a chance I can join immediately and I promise my honesty and 100 percent effort for the work.
FastAPI + MongoDB (PyMongo). All application code is in `app/main.py`.


## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```
Indexes are created at startup (idempotent). Requires MongoDB 6.0+ (uses `$setWindowFields`, `$unionWith`, `$dateAdd`, `$lookup` with `localField` + `pipeline`).

## Notes
* Instants are epoch milliseconds in the API and BSON datetimes in MongoDB (client uses `tz_aware=True`).
* `moving_avg_7d` averages the **reported** (4-decimal) `attendance_rate` values, as the contract words it.
* The trend fills missing days inside the database (`$unionWith` of a `$range`-generated day list), not with `$densify`, so a range with no records at all still returns one row per day.
* Concurrent PATCHes use optimistic concurrency (guarded update + up to 5 retries, then 409).
* Review findings: `REVIEW.md`. Design answers: `DECISIONS.md`.
