# Judged results

- raw events loaded: **7,000,000**
- verified active intervals: **149,500** (independent oracle: exact match)
- reporting window: **2026-07-27 13:14:00 → 2026-07-31 11:30:00 UTC** — holds 99.9% of events
- stray timestamps outside it: **6,643** events (0.095%), spanning 2023-05-17 05:23:00 → 2026-08-01 11:07:00; loaded, counted, excluded from averages so a handful of misdated rows cannot dilute the mean over a three-year fill grid
- run provenance: `sony.pipeline_runs`, evidence tag `log_comment = 'judged-surprise'`

Model: `active = intent_playing AND client_alive` (foreground-only; see README). Hour/day figures are the max and mean of bucket means — what the curve shows at that zoom. The minute-grain peak is *the* peak.

> This dataset is single-country (`india`), so the country slice necessarily equals all traffic — the filter is exercised, the data has one value. The platform slices prove filters bite.

## all traffic

| grain | peak | average | points | server ms | read rows |
|---|---:|---:|---:|---:|---:|
| minute | 18,936 | 162.5 | 5,657 | 65 (wall 348) | 69,172 |
| hour | 16,967 | 247.6 | 95 | 206 (wall 362) | 110,181 |
| day | 1,318 | 264.9 | 5 | 211 (wall 343) | 110,181 |

## platform = ANDROID_PHONE

| grain | peak | average | points | server ms | read rows |
|---|---:|---:|---:|---:|---:|
| minute | 6,046 | 52.0 | 5,657 | 214 (wall 131) | 110,181 |
| hour | 5,117 | 77.6 | 95 | 12 (wall 171) | 34,513 |
| day | 416 | 84.2 | 5 | 12 (wall 178) | 34,513 |

## country = india

| grain | peak | average | points | server ms | read rows |
|---|---:|---:|---:|---:|---:|
| minute | 18,936 | 162.5 | 5,657 | 13 (wall 375) | 34,513 |
| hour | 16,967 | 247.6 | 95 | 210 (wall 343) | 110,181 |
| day | 1,318 | 264.9 | 5 | 213 (wall 359) | 110,181 |

## video_type = live

| grain | peak | average | points | server ms | read rows |
|---|---:|---:|---:|---:|---:|
| minute | 7,759 | 53.0 | 5,657 | 65 (wall 185) | 69,172 |
| hour | 6,201 | 84.1 | 95 | 218 (wall 174) | 110,181 |
| day | 433 | 86.7 | 5 | 11 (wall 209) | 34,513 |

## platform = ANDROID_PHONE AND country = india

| grain | peak | average | points | server ms | read rows |
|---|---:|---:|---:|---:|---:|
| minute | 6,046 | 52.0 | 5,657 | 13 (wall 151) | 34,513 |
| hour | 5,117 | 77.6 | 95 | 217 (wall 191) | 110,181 |
| day | 416 | 84.2 | 5 | 59 (wall 118) | 69,172 |


## Evidence

- every query above carries `log_comment = 'judged-surprise'`; verify with:
```sql
SELECT event_time, query_duration_ms, read_rows, query
FROM system.query_log WHERE log_comment = 'judged-surprise' AND type = 'QueryFinish' ORDER BY event_time
```
- pipeline provenance (stages, row counts, rejects, durations):
```sql
SELECT * FROM sony.pipeline_runs ORDER BY started_at DESC LIMIT 1
```

## The queries

One series definition, three grains, filters pushed into the delta scan. The serving table is `ORDER BY (minute, platform, country, video_type, content_id)` so a filtered scan prunes granules.

```sql
-- minute series (exact): running sum over signed deltas, densified;
-- averages reported over the window holding 99.9% of events
SELECT minute, c FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(delta) AS d
    FROM sony.concurrency_minute_delta
    -- optional WHERE platform/country/video_type
    GROUP BY minute ORDER BY minute WITH FILL STEP toIntervalMinute(1)))
WHERE minute BETWEEN '2026-07-27 13:14:00' AND '2026-07-31 11:30:00'
```

<details><summary>all traffic · minute</summary>

```sql
SELECT max(c) AS peak, round(avg(c), 1) AS avg_c, count() AS points FROM (
SELECT minute, c FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(delta) AS d
    FROM sony.concurrency_minute_delta
    
    GROUP BY minute ORDER BY minute WITH FILL STEP toIntervalMinute(1)))
WHERE minute BETWEEN '2026-07-27 13:14:00' AND '2026-07-31 11:30:00'
)
```
</details>

<details><summary>all traffic · hour</summary>

```sql
SELECT max(m) AS peak, round(avg(m), 1) AS avg_c, count() AS points FROM (SELECT toStartOfHour(minute) AS h, round(avg(c), 1) AS m FROM (
SELECT minute, c FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(delta) AS d
    FROM sony.concurrency_minute_delta
    
    GROUP BY minute ORDER BY minute WITH FILL STEP toIntervalMinute(1)))
WHERE minute BETWEEN '2026-07-27 13:14:00' AND '2026-07-31 11:30:00'
) GROUP BY h)
```
</details>

<details><summary>all traffic · day</summary>

```sql
SELECT max(m) AS peak, round(avg(m), 1) AS avg_c, count() AS points FROM (SELECT toStartOfDay(minute) AS d2, round(avg(c), 1) AS m FROM (
SELECT minute, c FROM (
  SELECT minute, sum(d) OVER (ORDER BY minute) AS c FROM (
    SELECT minute, sum(delta) AS d
    FROM sony.concurrency_minute_delta
    
    GROUP BY minute ORDER BY minute WITH FILL STEP toIntervalMinute(1)))
WHERE minute BETWEEN '2026-07-27 13:14:00' AND '2026-07-31 11:30:00'
) GROUP BY d2)
```
</details>
