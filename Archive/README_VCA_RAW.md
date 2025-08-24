# VCA Raw Extract

## Files
- `vca_raw_extract.sql` — your approved raw SQL (no formatting/restructuring).
- `run_vca_extract.py` — runs the SQL file and saves the result to CSV.

## Usage
Set connection with either a DB URL or PG* env vars.

### Option A: DB URL
python run_vca_extract.py --sql vca_raw_extract.sql --out vca_raw_extract.csv --db "postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME"

### Option B: Environment variables
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=postgres
export PGDATABASE=postgres

python run_vca_extract.py --sql vca_raw_extract.sql --out vca_raw_extract.csv
