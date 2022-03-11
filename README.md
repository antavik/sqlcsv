# SQLCSV
POC for querying CSV via SQL powered by SQLite.

### Setup
1. Put compiled SQLite CSV extension in `/sqlcsv/ext` folder.
2. Import lib in python script `import sqlcsv`.

### Endpoints
- `read(filepath, tablename)` – to read a CSV file.
- `execute(query)` – to execute query on a CSV file.