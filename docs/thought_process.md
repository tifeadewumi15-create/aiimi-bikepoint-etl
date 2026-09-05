# Thought Process Log

Running notes on decisions made throughout the build — this becomes the source
material for the final PowerPoint. Add to it as we go, don't leave it to the end.

## 2026-09-03 — Kickoff
- Task: ETL pipeline for TfL BikePoint API in Databricks (PySpark + SQL).
- Client framing: Mayor of London deciding where to invest in more bike points.
- Platform choice: Databricks Community Edition (free), notebooks combine PySpark
  and SQL cells natively — matches the brief's requirement to show both languages.
- Version control: local Git repo, notebooks exported as source-formatted .py files
  so diffs are readable (Databricks Repos / "Git folders" supports this natively).

## Ideas for "if I had more time" (brief explicitly invites this in a markdown cell)
- Live/scheduled pipeline: TfL's BikePoint API only gives a single point-in-time
  snapshot. For a genuine investment-planning analysis I'd want to schedule this
  notebook to run repeatedly (e.g. hourly, via a Databricks Job) and append each
  pull into a history table, so patterns in demand over time (which stations are
  reliably empty/full at which times) could be analysed, not just current state.
  Decision for the test itself: build against a single snapshot, note the
  limitation explicitly rather than spend limited time building the scheduling.

## 2026-09-03 — Extract (Bronze) complete
- Converted the raw `data` list (800 bike points) into a Spark DataFrame and
  saved it as Delta table `bronze_bikepoints`.
- Hit [CANNOT_DETERMINE_TYPE]: `children` and `childrenUrls` are always empty
  lists for BikePoint entries, Spark can't infer an element type from an
  array that's never populated. Dropped both fields before creating the
  DataFrame, they carry no useful info for this analysis anyway.
- Preview showed "712+ rows, Truncated data" after the fix, looked like a
  problem but was just a display limit. Confirmed via bronze_df.count() -> 800,
  matching the original API count exactly. No data lost.
- Bronze (raw, untouched) layer is done. Next: Silver (Transform) - flatten
  additionalProperties into real columns, cast string values to numeric types.

## 2026-09-03 — Silver (Transform) in progress
- Flattened additionalProperties: explode (list -> one row per attribute) then
  groupBy + pivot(attribute_name) + agg(first(attribute_value)) (rows -> columns).
  Result: 800 rows, one per bike point, genuinely flat.
- Cast numeric fields (NbBikes, NbDocks, NbEBikes, NbEmptyDocks, NbStandardBikes)
  to int, and Installed/Locked/Temporary to boolean. Were all strings after pivot.
- Data quality finding: 3 bike points have a non-blank RemovalDate despite
  Installed=true and Locked=false (i.e. looks currently active). RemovalDate
  could be stale historical metadata from a past removal that was reinstalled,
  OR TfL's backend genuinely hasn't synced a real removal yet, can't tell
  which from the data alone. Given the analysis feeds an investment decision,
  decided to exclude these 3 rather than assume they're fine: the risk of
  recommending investment near a station that's actually being decommissioned
  outweighs the loss of 3 rows out of 800. Kept them in a separate
  `excluded_stations` DataFrame rather than silently dropping them, so there's
  a record of what was removed and why.
- Still to do: convert InstallDate/RemovalDate from Unix ms timestamps to real
  dates, then save the cleaned result as Delta table silver_bikepoints.
- Silver (Transform) complete. Saved silver_bikepoints (797 rows) and
  excluded_bikepoints (3 rows, the RemovalDate anomalies) as Delta tables.

## 2026-09-04 — Gold (Load) in progress
- Built gold_station_utilization: for every station, % of docks currently
  holding a bike (pct_bikes_available) and % of docks currently empty
  (pct_docks_available). Low values on either flag a station under strain
  right now, can't rent (low bikes) or can't return (low empty docks).
- Data quality finding: gold_station_utilization has 796 rows, not 797.
  WHERE NbDocks > 0 (added to avoid divide-by-zero) excluded exactly one
  station, BikePoints_612, which has NbBikes = 0, NbEmptyDocks = 0, and
  NbDocks = 0. Every count is zero, a different anomaly to the RemovalDate
  one, a station reporting no real capacity at all. Correctly excluded,
  percentage calculations would be meaningless for it anyway.
- Investigated a mismatch (NbBikes + NbEmptyDocks != NbDocks for 547/796
  stations) and tested whether e-bikes explained it, they didn't (gap was
  similar with/without e-bikes present, 1.24 vs 1.45). Decided to cut this
  thread from the final notebook/presentation, not essential to the Mayor's
  question and would need more time to investigate properly. Noted here only
  so it isn't mistaken for a dropped/forgotten finding later.
- Final Gold output: gold_station_utilization table, plus a ranked top-20
  worst-strain stations query (station_label zero-padded for correct chart
  sort order), with a bar chart visualization. Headline stat (6.2%) also
  dropped, decided it didn't add value beyond the ranked list itself.
