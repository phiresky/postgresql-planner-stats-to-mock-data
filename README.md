
See the following article for more details:

https://...

## Running

Setup:

```
npm install
```

### Step 1: Extract stats from production

You can supply the password via an env var or directly in the connection URI.
```bash
PGPASSWORD=xxx npm run extract-postgresql-stats postgresql://user@production [configFilePath] [schema] [table]
```

The config file should follow [the definition in config.ts](./config.ts). By default, it will extract all tables and columns.

This script will output the following file structure

```
schema_with_samples
├── public
│   └── users.sql
└── tables.json
```

The tables.json is the most important part and the input to step 2. The individual SQL files contain samples from the tables and statistical information about them which might be useful.

### Step 2: Generate data and insert into database

```bash
npm run insert-mock-data postgresql://user@staging schema_with_samples/tables.json [configFile]
```

This will generate data based on the extracted information and insert it into the given database.