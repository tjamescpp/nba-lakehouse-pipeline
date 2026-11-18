# NBA Lakehouse Pipeline

An incremental ETL pipeline on Databricks that ingests the 2026-27 NBA season from a public
API, validates it, and builds analysis-ready tables, using a Bronze / Silver / Gold
(medallion) architecture on Delta Lake. It runs daily as a scheduled Databricks job.

**What it demonstrates**
- Resilient ingestion: retries with exponential backoff, idempotent landing, incremental loads
- Spark transformations: JSON parsing with explicit schemas, nested `explode`, window functions
- Data quality gates that stop the pipeline before bad data is written
- Orchestration with Databricks Jobs: task dependencies, a daily schedule, retries, and alerts

## Architecture

```mermaid
flowchart LR
    A[ESPN public API] --> B[Raw JSON files<br/>Unity Catalog volume]
    B --> C[Bronze<br/>raw text plus metadata]
    C --> D[Silver<br/>parsed, typed, validated]
    D --> E[Gold<br/>aggregated for analysis]
```

The job `nba_pipeline` runs three notebook tasks in order, each reading from the layer below:

| Task | Notebook | Reads | Writes |
|---|---|---|---|
| `bronze_ingest` | `01_bronze_ingest` | ESPN API | `bronze_scoreboard`, `bronze_game_summary` |
| `silver_transform` | `02_silver_transform` | Bronze tables | `silver_games`, `silver_player_box_scores` |
| `gold_aggregates` | `03_gold_aggregates` | Silver tables | `gold_team_standings`, `gold_player_season_stats`, `gold_team_rolling_form` |

If a task fails, the tasks after it are skipped and the tables they would have written keep
their previous contents.

## Tables

| Table | One row per | Purpose |
|---|---|---|
| `bronze_scoreboard` | raw scoreboard file (one date) | Untouched API response plus `source_file` and `ingested_at` |
| `bronze_game_summary` | raw game file | Untouched box score response plus metadata |
| `silver_games` | final game | Date, teams, and scores, with proper types |
| `silver_player_box_scores` | player per game | Minutes and box score stats; players who did not play have null stats |
| `gold_team_standings` | team | Record, home and away splits, point differential |
| `gold_player_season_stats` | player | Totals, per-game averages, shooting percentages |
| `gold_team_rolling_form` | team per game | Record and scoring over the last 10 games as of each game |

## Design decisions

| Decision | Why | Trade-off |
|---|---|---|
| Bronze stores the raw response text, not parsed columns | If Silver logic has a bug, I can reprocess from Bronze without calling the API again | Bigger tables, and parsing is deferred to Silver |
| Raw files land in a volume first, then load into Bronze tables | Files are a replayable source of truth, and re-running never creates duplicates because file names come from the date or game ID | Bronze is rebuilt with `overwrite` from every file each run. That is simple and idempotent at this size, but at larger scale I would switch to Auto Loader or `COPY INTO` to load only new files |
| Scoreboards are overwritten; box scores are skipped if already landed | A date's results can still change, but a final game's box score will not | Fewer API calls, and a stat correction to an old final game would not be picked up |
| Each run re-fetches the last 3 days, not just yesterday | Catches late finishers and corrections, and retries any day that failed last time | A few redundant requests |
| `game_date` comes from the requested date, not the UTC tip-off time | Late US games cross midnight UTC and would be filed under the wrong day | The date column and the tip-off timestamp can legitimately differ |
| Silver parses JSON with an explicit schema | Reads only the fields I need and ignores the rest of a roughly 500 KB response | New fields from the source are ignored until I add them |
| Stats are mapped by position, guarded by a check on the label order | ESPN sends each player's stats as a positional list of strings | If ESPN reorders the stats, the run stops instead of putting numbers in the wrong columns |
| Quality gates raise before each write | A failed check stops the task, so bad data never reaches the table and downstream tasks are skipped | A single bad row blocks the run until I look at it |
| Gold tables are rebuilt from Silver on every run | They are small, so a full rebuild is simpler and safer than incremental updates | Would not scale to many seasons without rethinking |
| Retries only on the Bronze task | Bronze talks to the outside world, so its failures are often transient. Silver and Gold failures are deterministic, and retrying would just fail again | A genuinely broken Bronze run takes a few extra minutes to alert |
| Scheduled for 12:00 UTC | The previous night's last West Coast games are final by then | Runs after the US morning, not near real time |

## Problems I hit

**Both NBA-owned APIs blocked Databricks.** My first source, `stats.nba.com` (via the `nba_api` package), timed out from the workspace, and the NBA's CDN returned a 403 "Access Denied" from its bot protection. Both appear to block cloud IP ranges, so retries could not help. I switched to ESPN's public endpoints, which worked. Only the ingestion client changed, because the rest of the pipeline was built around raw files, which is the payoff of separating extraction from transformation.

**One player row broke a quality check.** My checks flagged one player, out of roughly 5,300 rows, who ESPN marked as having played but whose minutes were `--` and whose stats were all zero. ESPN's `didNotPlay` flag was unreliable here, as were other flags I looked at. I treated the exact `--` minutes pattern as did-not-play and left every check unchanged, so a different surprise would still be caught.

**A hard-coded date range would have broken the first incremental run.** A check that said "no games after the day I backfilled" would have failed the moment a new game landed. I replaced it with a range from the season opener to the current date, and filtered Silver to final games so postponed games cannot block the pipeline.

## Setup

Built and tested on Databricks Free Edition (serverless compute, Unity Catalog).

1. **Clone the repo** into a Databricks Git folder (Workspace, then Create, then Git folder).
2. **Create the schema and volume.** Run this once in a notebook, adjusting the catalog name
   if yours is not `workspace`:
   ```sql
   -- Schema that holds every table in the project
   CREATE SCHEMA IF NOT EXISTS workspace.nba;
   -- Volume that holds the raw API responses
   CREATE VOLUME IF NOT EXISTS workspace.nba.raw;
   ```
3. **Backfill the season once.** Open `notebooks/01_bronze_ingest`, set the `start_date` widget
   to `2026-10-20` and `end_date` to a recent date, and run it. A full backfill makes about
   one API call per game plus one per day, with a pause between calls, so it takes several minutes.
4. **Run the rest once.** Run `02_silver_transform`, then `03_gold_aggregates`.
5. **Create the job.** Make a Databricks job named `nba_pipeline` with three notebook tasks:
   `bronze_ingest`, then `silver_transform`, then `gold_aggregates`, each depending on the
   one before it. Give `bronze_ingest` 2 retries (5 minutes apart) and a 60-minute timeout.
   Add a daily schedule at 12:00 UTC and a failure notification.

After the backfill, leave the date widgets blank. The job then fetches the last 3 days
ending yesterday (UTC) each run.

## Limitations

- **The data source is unofficial.** ESPN's endpoints are undocumented and can change
  without notice. The label-order check and the quality gates are there to turn that
  kind of change into a stopped run instead of bad data.
- **Season to date only.** The pipeline covers the current season from opening night.
  Conference and division standings are not modeled, because the source data used
  does not include them.
- **Full rebuilds.** Bronze, Silver, and Gold are rebuilt from complete inputs on every run.
  This is simple and idempotent at this size, but would need incremental loading
  (for example Auto Loader and `MERGE`) for many seasons.
- **Free Edition constraints.** There is no uptime guarantee, compute is serverless only,
  and exceeding the fair-usage quota can block runs for the rest of the day.
  Free Edition is intended for learning and personal use, not production workloads.
- **No automated tests yet.** Correctness is enforced by the runtime quality gates,
  not by a unit test suite.

## Possible next steps

These are ideas, not implemented:

- Unit tests for `quality.py` and the API client's retry logic
- A dashboard on the Gold tables
- Delta table constraints as a second layer of validation
- Auto Loader for incremental Bronze loading
- A CI check that runs the tests on every push
**A transient storage error during a job run.** One run failed with a volume error (`Errno 126`) while writing a file. A re-run succeeded, so I added a retry on the Bronze task, and the pipeline raises before loading Bronze if any fetch failed, so downstream tasks never run on incomplete data.

**Dividing by zero.** Databricks runs Spark in ANSI mode, where division by zero and bad casts raise errors instead of returning null. I used `try_cast` and `try_divide` so missing data becomes null, which is what "no attempts" and "did not play" should be.
