# FIFA World Cup Data Pipeline (Databricks, PySpark)

An end-to-end data engineering project built in Databricks. It takes four messy CSV files, cleans them with PySpark, and turns them into football insights using the **medallion architecture** (bronze, silver, gold).

> **Note:** The data is made up for practice (fake players, scores, coaches and stadiums). Country names are real, but nothing here describes a real tournament.

---

## Table of contents

1. [Project overview](#project-overview)
2. [Architecture](#architecture)
3. [Source data](#source-data)
4. [Bronze layer](#bronze-layer)
5. [Silver layer](#silver-layer)
6. [Gold layer](#gold-layer)
7. [Charts](#charts)
8. [Repository structure](#repository-structure)
9. [How to run](#how-to-run)
10. [Skills demonstrated](#skills-demonstrated)
11. [Lessons learned](#lessons-learned)

---

## Project overview

| Item | Detail |
|---|---|
| Platform | Databricks (Serverless) |
| Language | Python (PySpark) and SQL |
| Storage | Delta tables in Unity Catalog (`ecommerce.fifa`) |
| Raw files | Stored in a Unity Catalog volume (`/Volumes/ecommerce/fifa/raw_data/`) |
| Version control | Databricks Git folder linked to GitHub |

## Architecture

```
Raw CSV files  ->  Bronze  ->  Silver  ->  Gold  ->  Charts
 (volume)        (raw copy)   (cleaned)   (insights)
```

| Layer | Purpose | Tables |
|---|---|---|
| Bronze | Exact copy of the source, every column kept as text | `bronze_teams`, `bronze_players`, `bronze_matches`, `bronze_goals` |
| Silver | Cleaned, typed, de-duplicated, with flag columns for bad data | `silver_teams`, `silver_players`, `silver_matches`, `silver_goals` |
| Gold | Business-ready answers | `gold_top_scorers`, `gold_team_standings`, `gold_goals_by_time`, `gold_goals_by_position`, `gold_stadium_attendance` |

## Source data

| File | Rows (raw) | Rows (silver) | Contents |
|---|---|---|---|
| Teams.csv | 34 | 32 | 32 national teams, plus 2 duplicates |
| Players.csv | 326 | 320 | 10 players per team, plus 6 duplicates |
| Matches.csv | 64 | 63 | Group stage to final, plus 1 duplicate |
| Goals.csv | 176 | 172 | Every goal scored, plus 4 duplicates |

The files were generated with deliberate mistakes, so the cleaning steps had real problems to solve.

---

## Bronze layer

**Goal:** store the raw data exactly as it arrived, so any cleaning mistake can be fixed by rebuilding silver.

- One reusable function (`ingest_bronze`) loads every file.
- Schema inference is turned off, so every column stays text and nothing is lost or nulled.
- Two metadata columns are added: `_source_file` (where the row came from) and `_ingested_at` (when it was loaded).
- `overwrite` mode is used, so re-running never creates duplicate rows.

```python
def ingest_bronze(file_name, table_name):
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)
          .option("encoding", "UTF-8")
          .csv(f"/Volumes/ecommerce/fifa/raw_data/{file_name}")
          .withColumn("_source_file", col("_metadata.file_path"))
          .withColumn("_ingested_at", current_timestamp()))
    df.write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"ecommerce.fifa.bronze_{table_name}")
```

---

## Silver layer

**Goal:** make the data correct and consistent. Bad values are labelled with a status or flag column instead of being silently deleted.

### silver_teams

| Column | Problem | Fix |
|---|---|---|
| `team_name` | `egypt`, `" Belgium "`, `Usa` | `trim` and `initcap`, with USA restored |
| `confederation` | `caf`, `Africa`, `Europe` | Upper case, mapped to official codes (UEFA, CAF, CONMEBOL, AFC) |
| `fifa_ranking` | `#14`, `N/A`, empty | Removed symbols, cast to integer, missing kept as null |
| `coach_name` | Wrong capitals | `trim` and `initcap` |
| `team_id` | Duplicate teams | `dropDuplicates` (34 to 32) |

### silver_players

| Column | Problem | Fix |
|---|---|---|
| `position` | `GK`, `Keeper`, `FW`, `Striker` | Mapped to 4 positions with `isin` |
| `first_name`, `last_name` | Wrong capitals, spaces | Cleaned, then `full_name` created |
| `date_of_birth` | 4 date styles, empty values | Tried each format with `coalesce`, cast to date |
| `is_future_dob`, `age` | Birthdays in 2028 | Flagged, age calculated |
| `height_cm` | `190 cm`, `190cm`, empty | Digits only, cast to integer |
| `market_value_eur` | `€15M`, `€500K`, `-500000` | Converted to full numbers, negatives set to null, `value_status` label added |
| `player_id` | Duplicate players | `dropDuplicates` (326 to 320) |
| `team_id` | Fake team `T099` | `is_valid_team` flag |

### silver_matches

| Column | Problem | Fix |
|---|---|---|
| `match_date` | 4 date styles | `coalesce` with `try_to_timestamp`, cast to date |
| `home_team_id` | Lower-case IDs (`t011`) | `trim` and `upper` |
| `home_score`, `away_score` | `-` for unknown | `has_score` flag made first, then cast, nulls filled with 0 |
| `stage` | `QF`, `R16`, `group stage` | Mapped to 5 clean stages |
| `group_name` | Null for knockout games | Filled with `Knockout` |
| `attendance` | `73,371`, `-1`, empty | Commas removed, `attendance_status` label added |
| `match_id` | Duplicate match | `dropDuplicates` (64 to 63) |

### silver_goals

| Column | Problem | Fix |
|---|---|---|
| `goal_type` | `OWN GOAL`, `header` | `initcap`, 5 clean types |
| `minute` | `59'`, `45+2`, `90+5` | Split into `minute` and `added_time`, `is_added_time` flag |
| `goal_id` | Duplicate goals | `dropDuplicates` (176 to 172) |
| `player_id`, `match_id` | Fake player `PL9999` | `is_valid_player` and `is_valid_match` flags |

---

## Gold layer

**Goal:** answer business questions from the clean silver tables.

| Gold table | Question it answers | Techniques |
|---|---|---|
| `gold_top_scorers` | Who are the top scorers? | Filter, `groupBy`, `inner` and `left` joins, `dense_rank` |
| `gold_team_standings` | Who leads on points and goal difference? | Home and away `unionByName`, conditional sums, `row_number` |
| `gold_goals_by_time` | In which part of a match are most goals scored? | `when` time groups, `groupBy` |
| `gold_goals_by_position` | Which positions score the most? | Join, window percentage |
| `gold_stadium_attendance` | Which stadiums draw the biggest crowds? | Filter on `Valid` attendance, `avg` |

### Business rules used in gold

- Own goals are excluded from top scorers, because they do not count for the scorer.
- Standings use only matches where `has_score` is true, so filled-in `0-0` scores are not counted as draws.
- Average attendance uses only rows where `attendance_status` is `Valid`, so filled-in zeros do not drag the average down.
- Points: win = 3, draw = 1, loss = 0.

### Joins explained

| Join | Used for | Why |
|---|---|---|
| `inner` (goals to players) | Top scorers | A goal with no real player is useless, so it is dropped |
| `left` (scorers to teams) | Team names | A real scorer is kept even if their team is missing |
| `left_anti` | Data checks | Finds goals that point to a player that does not exist |

### Window functions

| Function | Behaviour on ties |
|---|---|
| `row_number` | Never ties: 1, 2, 3, 4 |
| `rank` | Ties share a number and the next is skipped: 1, 2, 2, 4 |
| `dense_rank` | Ties share a number and nothing is skipped: 1, 2, 2, 3 |

`partitionBy("team_id")` ranks players inside their own team, which gives each team's top scorer.

---

## Charts

Charts were built from the gold tables with Databricks visualizations.

### Average attendance by stadium

![Average attendance by stadium](stadium_attendance.png)

_Add more charts here, for example goals by time and top scorers._

---

## Repository structure

```
.
├── README.md
├── 01_bronze.py
├── 02_silver_teams.py
├── 03_silver_players.py
├── 04_silver_matches.py
├── 05_silver_goals.py
├── 06_gold.py
└── stadium_attendance.png
```

## How to run

1. In Databricks, create the schema and volume:
   ```sql
   CREATE SCHEMA IF NOT EXISTS ecommerce.fifa;
   CREATE VOLUME IF NOT EXISTS ecommerce.fifa.raw_data;
   ```
2. Upload `Teams.csv`, `Players.csv`, `Matches.csv` and `Goals.csv` to `/Volumes/ecommerce/fifa/raw_data/`.
3. Run the notebooks in this order:

| Order | Notebook | Creates |
|---|---|---|
| 1 | `01_bronze` | 4 bronze tables |
| 2 | `02_silver_teams` | `silver_teams` |
| 3 | `03_silver_players` | `silver_players` |
| 4 | `04_silver_matches` | `silver_matches` |
| 5 | `05_silver_goals` | `silver_goals` |
| 6 | `06_gold` | 5 gold tables |

`05_silver_goals` must run after players and matches, because it checks goals against both tables.

## Skills demonstrated

- Medallion architecture (bronze, silver, gold)
- PySpark DataFrame transformations
- Data cleaning: dates, text, numbers, duplicates, missing values
- Safe conversions with `try_cast` and `try_to_timestamp`
- Joins: `inner`, `left`, `left_anti`
- Window functions: `row_number`, `rank`, `dense_rank`, `partitionBy`
- Data quality flags instead of deleting bad rows
- Delta tables and Unity Catalog
- Git integration with Databricks Git folders
- Data visualization

## Lessons learned

- **Run notebook cells in order.** Most errors (`df is not defined`, `column not found`) came from skipped or restarted cells.
- **Create flag columns before replacing values.** After `-` becomes `0`, only a `has_score` label can tell a missing score from a real 0-0.
- **Never use `[^0-9]` on values that can be negative or contain `+`.** It turns `-1` into `1` and `45+2` into `452`.
- **Keep bronze raw.** Fixing mistakes in silver means bronze can always rebuild it.
- **Use `inner` when a row is useless without a match, and `left` when it matters on its own.**

## Author

Giddanti Shankar | [LinkedIn](www.linkedin.com/in/giddanti-shankar-541b7a217) | [GitHub](https://github.com/shankarleo)
