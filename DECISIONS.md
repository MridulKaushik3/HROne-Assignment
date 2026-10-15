# DECISIONS.md

1. **Indexes.** `employees.emp_code` unique and `attendance_logs(emp_code, date)` unique: they make duplicates impossible and serve PATCH and monthly lookups. `attendance_logs(date desc, emp_code)` serves the list order and month-wide analytics. `(emp_code, punch_in desc)` finds the record to punch out. Employee indexes on `department` and `joined_on` serve headcount. Rejected: a `status` index, a weak filter that the date index already narrows.
2. **Punch-in race.** Both requests compute the same date and call `insert_one`. The unique `(emp_code, date)` index lets MongoDB accept one; the other gets `DuplicateKeyError`, returned as 409. No find-then-insert, so no gap between check and write.
3. **Ties.** `limit` applies to the rank, not the row count. `$rank` gives tied employees the same rank, so everyone tied at the cutoff is returned and the list can exceed `limit`.
4. **Headcount.** The summary starts from `employees` (joined on or before month end) and `$lookup`s their logs, keeping employees with none. A first `$group` per employee makes headcount a count of people, not logs.
5. **100x data.** Keep daily per-department rollup documents, updated on write, so analytics read small summaries instead of scanning raw records.
