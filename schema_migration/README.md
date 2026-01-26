# Azure DocumentDB Schema Migration

Automated schema creation for migrating from MongoDB Atlas to Azure DocumentDB based on assessment reports.

## Overview

This tool reads an Azure DocumentDB assessment report (HTML) and creates the corresponding databases and collections in your target DocumentDB instance based on a customizable migration configuration.

## Files

| File | Description |
|------|-------------|
| `assessmentreport_atlas_sample.html` | Source assessment report from Azure DocumentDB migration assessment |
| `migration_config.json` | Configuration file to control what gets migrated |
| `create_schema.py` | Python script to create the schema in DocumentDB |

## Quick Start

### 1. Configure Connection

Copy the template and update with your DocumentDB connection string:

```bash
cp .env.template .env
```

Edit `.env` file:
```bash
MONGODB_CONNECTION_STRING=mongodb://username:password@your-endpoint.mongocluster.cosmos.azure.com:27017/?tls=true
TIMEOUT_SECONDS=60
```

### 2. Parse Assessment Report

Convert the HTML assessment report to JSON:

```bash
python parse_assessment.py --input assessmentreport_atlas_sample.html --output schema.json
```

This creates `schema.json` with all databases and collections.

### 3. Review and Edit Schema (Optional)

Open `schema.json` and remove any databases or collections you don't want to migrate.

Example - remove a collection:
```json
{
  "databases": [
    {
      "database": "sample_mflix",
      "collections": [
        {"name": "movies", "doc_count": 21349, "data_gb": 0.032},
        // Remove this line to skip migration:
        // {"name": "old_data", "doc_count": 1000, "data_gb": 0.001}
      ]
    }
  ]
}
```

### 4. Create Schema in DocumentDB

```bash
python generate_mongosh_script.py --schema schema.json
```

**Optional:** Add a database prefix:
```bash
python generate_mongosh_script.py --schema schema.json --db-prefix prod_
```

The script automatically connects to DocumentDB and creates all databases/collections.

## Configuration Options

### Database Selection

**Migrate all collections in a database:**
```json
"sample_airbnb": {
  "migrate": true,
  "collections": ["*"]
}
```

**Migrate specific collections only:**
```json
"sample_mflix": {
  "migrate": true,
  "collections": ["movies", "users", "comments"]
}
```

**Skip a database entirely:**
```json
"sample_guides": {
  "migrate": false,
  "collections": []
}
```

### Options

```json
"options": {
  "create_indexes": true,      // Create indexes (future feature)
  "shard_collections": false,  // Shard collections (future feature)
  "dry_run": true              // Test mode - no actual changes
}
```

### Database Prefix

Add a prefix to all database names in the target:

```json
"target": {
  "database_prefix": "prod_"   // Creates prod_sample_airbnb, prod_sample_mflix, etc.
}
```

## Assessment Report Summary

Based on `assessmentreport_atlas_sample.html`:

| Database | Collections | Total Size | Do`.env` file:

```bash
DATABASE_PREFIX=prod_
```

This creates `prod_sample_airbnb`, `prod_sample_mflix`, etc.ample_mflix | 6 | 0.094 GB | 67,661 |
| sample_restaurants | 2 | 0.013 GB | 25,554 |
| sample_supplies | 1 | 0.004 GB | 5,000 |
| sample_training | 7 | 0.111 GB | 296,502 |
| sample_weatherdata | 1 | 0.016 GB | 10,000 |

**Total: 9 databases, 23 collections, 425,367 documents**

## Example Output

**Step 1: Parse Assessment**
```
✅ Parsed assessment report: 23 collections found
✅ Schema JSON saved to: schema.json

📊 Summary:
   Databases: 9
   Collections: 23

   📁 sample_airbnb
      Collections: 1
         - listingsAndReviews (5,555 docs, 0.088 GB)

   📁 sample_mflix
      Collections: 6
         - movies (21,349 docs, 0.032 GB)
         - users (185 docs, 0.000 GB)
         ...

💡 Next step:
   1. Review/edit schema.json to remove unwanted collections
   2. Run: python generate_mongosh_script.py --schema schema.json
```

**Step 2: Edit schema.json (optional)**
Remove unwanted collections from the JSON file.

**Step 3: Create Schema**
```
✅ Loaded schema: schema.json

📊 Schema Summary:
   Databases: 9
   Collections: 23

   📁 sample_airbnb
      Collections: 1
         - listingsAndReviews

🔌 Connecting to DocumentDB and creating schema...
**Execution Output:**
```
============================================================
Creating Schema in DocumentDB
============================================================
Total Databases: 9
Database Prefix: None
============================================================

📁 Creating database: sample_airbnb
   ✅ Created: listingsAndReviews
      - Expected Docs: 5,555
      - Data Size: 0.088 GB

📁 Creating database: sample_mflix
   ✅ Created: movies
      - Expected Docs: 21,349
      - Data Size: 0.032 GB
   ✅ Created: users
      - Expected Docs: 185
      - Data Size: 0.000 GB
...

============================================================
Schema Creation Complete
============================================================
```

## Next Steps

After creating the schema:

1. **Load Data** - Use MongoDB migration tools:
   - Azure Data Factory
   - mongodump/mongorestore
   - MongoDB Database Tools
   - Custom scripts

2. **Create Indexes** - Use index definitions from assessment report

3. **Verify Migration** - Compare document counts and data sizes

4. **Performance Testing** - Run query performance tests

## Notes

- This script only creates the database and collection structure
- Actual data migration requires separate tools (ADF, mongorestore, etc.)
- Index creation will be added in future version
- DocumentDB connection string format: `mongodb://endpoint:27017/?tls=true`
