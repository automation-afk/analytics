@echo off
set DATABASE_URL=postgresql://postgres:fNqgPnDjwyEPXneRIBmWBxneKSMRdaOI@yamabiko.proxy.rlwy.net:32154/railway
python scripts/migrate_to_postgres.py
