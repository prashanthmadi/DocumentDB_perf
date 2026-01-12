# Mobile Application Performance Testing for Azure DocumentDB

This project provides a scalable performance testing pattern for Azure DocumentDB (formerly Azure Cosmos DB for MongoDB vCore) and MongoDB, featuring automated data generation and configurable workload testing.

## 📋 Project Overview

The goal is to create a reusable performance testing framework where:
- Users provide a **sample JSON document** representing their data structure
- Users provide a **data generator notebook template**
- **GitHub Copilot** generates a customized data generator that matches the sample document
- Separate notebooks handle data generation and database insertion for clean separation of concerns

## 📁 Project Files

### Fabric Environment (Run in Microsoft Fabric Spark)

#### 01. Data Generation
- **`01_mobile_app_data_generator_pyspark.ipynb`** - PySpark-based data generator
  - Generates 20M to 200M+ records
  - Auto-scaling partitions and performance tuning
  - Writes to Delta Lake partitioned by `fields.state`
  - Configurable at top cell: `NUM_RECORDS`, `NUM_PARTITIONS`, `DELTA_TABLE_PATH`

#### 02. Data Insertion
- **`02_mongodb_data_insertion_pyspark.ipynb`** - Azure DocumentDB/MongoDB insertion
  - Reads from Delta Lake
  - Inserts data using Spark MongoDB Connector
  - Upsert operations with partition key optimization
  - Dynamic batch sizing

### Local Environment (Run on your machine)

#### 03. Index Creation
- **`03_create_mongodb_indexes.py`** - Create indexes from JSON
  - Reads index definitions from `data/mongodb_indexes.json`
  - Executes index creation via mongosh
  - Reports creation status and timing

#### 04. Query Execution (Server-Side Timing)
- **`04_query_performance_server_time.py`** - Execute queries and measure **server-side execution time**
  - Uses `explain("executionStats")` to capture actual database engine time
  - Reads queries from `data/mongodb_queries.json`
  - **Excludes network latency** - pure database performance
  - Outputs timing to CSV with column: `collectionName_epochTimestamp`

#### 05. Explain Plan Generation
- **`05_generate_explain_plans.py`** - Generate detailed query execution plans
  - Captures full explain output for query analysis
  - Uses `allPlansExecution` mode (falls back to `executionStats` on timeout)
  - Outputs to `data/explain_out_*.txt`

#### 06. Query Execution (Round-Trip Timing)
- **`06_query_performance_client_time.py`** - Execute queries and measure **end-to-end time**
  - Measures wall-clock time (client perspective)
  - **Includes network latency** - real-world performance
  - Outputs timing to CSV with column: `collectionName_direct_epochTimestamp`

> **⚡ Performance Timing Methods:**
> - **Script 04**: Server-side execution time from `executionStats` (database engine only)
> - **Script 06**: Round-trip time including network latency (client experience)
> - Use both to understand where time is spent: database vs. network

### Data Files
- **`data/mongodb_indexes.json`** - Index definitions in JSON format
- **`data/mongodb_queries.json`** - Query definitions with descriptions

## 🚀 Microsoft Fabric Spark Setup

### Prerequisites
- Microsoft Fabric Workspace with Spark enabled
- Azure DocumentDB account (or MongoDB Atlas)

### Custom Environment Setup

1. **Create Custom Fabric Environment**
   - In your Fabric workspace, create a new environment
   - Add Spark configuration properties:
     ```
     spark.driver.extraJavaOptions --add-modules jdk.naming.dns --add-exports jdk.naming.dns/com.sun.jndi.dns=java.naming
     spark.executor.extraJavaOptions --add-modules jdk.naming.dns --add-exports jdk.naming.dns/com.sun.jndi.dns=java.naming
     ```

2. **Upload MongoDB Spark Connector JAR**
   - Download `mongo-spark-connector_2.12-10.5.0-all.jar` from Maven Central
   - In your environment, go to **Custom libraries** tab
   - Upload the JAR file
   - Wait for **Status: Success**

3. **Attach Environment to Notebook**
   - Open your notebook
   - Select the custom environment you created

## 💾 Database Setup

### Create Database and Collections

Use mongosh to create your database and collections:

**Create Unsharded Collection:**
```javascript
use mobile_apps_0126;
db.createCollection("applications_unsharded");
```

**Create Sharded Collection:**
```javascript
// Enable sharding on database
sh.enableSharding("mobile_apps_0126");

// Create collection
use mobile_apps_0126;
db.createCollection("applications_sharded");

// Shard the collection
sh.shardCollection("mobile_apps_0126.applications_sharded", {"fields.state": "hashed"});

// Verify
db.applications_sharded.getShardDistribution();
```

## 🎯 Usage Workflow

### Step 1: Generate Data (Fabric Spark)
1. Open **`01_mobile_app_data_generator_pyspark.ipynb`** in Microsoft Fabric
2. Configure at the top cell:
   ```python
   NUM_RECORDS = 200000000      # Adjust based on test requirements
   NUM_PARTITIONS = 1000        # Auto-adjusts based on NUM_RECORDS
   DELTA_TABLE_PATH = "abfss://your-container@onelake.dfs.fabric.microsoft.com/..."
   ```
3. Run all cells to generate and save data to Delta Lake

### Step 2: Insert Data into Azure DocumentDB (Fabric Spark)
1. Open **`02_mongodb_data_insertion_pyspark.ipynb`** in Microsoft Fabric
2. Configure your connection:
   ```python
   conn_str = "mongodb+srv://username:password@cluster.mongodb.net/"
   DELTA_TABLE_PATH = "abfss://..."  # Same as Step 1
   ```
3. Run all cells to insert data

### Step 3: Create Indexes (Local Environment)
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Install [mongosh](https://www.mongodb.com/docs/mongodb-shell/install/) on your machine
3. Create `.env` file with connection details:
   ```
   MONGODB_CONNECTION_STRING=mongodb+srv://username:password@cluster.mongodb.net/
   MONGODB_DATABASE=mobile_apps
   MONGODB_COLLECTION=applications
   INDEXES_FILE=data/mongodb_indexes.json
   ```
4. Edit `data/mongodb_indexes.json` to define your indexes:
   ```json
   [
     {"name": "idx_state", "keys": {"fields.state": 1}},
     {"name": "idx_status_revenue", "keys": {"fields.status": 1, "fields.revenue": -1}}
   ]
   ```
5. Run the script:
   ```bash
   python 03_create_mongodb_indexes.py
   ```

### Step 4: Run Performance Tests (Local Environment)

#### Option A: Server-Side Timing (Script 04)
Measures pure database execution time using explain stats:
```bash
python 04_query_performance_server_time.py
```

#### Option B: Round-Trip Timing (Script 06)
Measures end-to-end time including network latency:
```bash
python 06_query_performance_client_time.py
```

#### Option C: Generate Explain Plans (Script 05)
Captures detailed execution plans for query analysis:
```bash
python 05_generate_explain_plans.py
```

1. Edit `data/mongodb_queries.json` to define your test queries:
   ```json
   [
     {
       "description": "Find all applications in California",
       "query": "targetDb.applications.find({'fields.state': 'CA'}).count()"
     }
   ]
   ```
2. Review results:
   - Console output shows execution time for each query
   - Results saved to `data/Query_Execution_output.csv`
   - Each run adds a new column with timing data
   - Compare performance across different runs and collections

## 📊 Data Schema

The generated mobile application data includes:

```json
{
  "_id": "app_1704729600000_001_00000001_abc123de",
  "applicationTimeStamp": "2024-01-08T10:30:00Z",
  "createdDateTime": "2024-01-08T10:30:00Z",
  "lastUpdateDateTime": "2024-01-08T10:35:00Z",
  "serviceId": "SVC123",
  "version": "v1.0.0",
  "_class": "com.mobile.Application",
  "fields": {
    "firstName": "James",
    "lastName": "Smith",
    "state": "CA",
    "zip": "90210",
    "deviceManufacturer": "Apple",
    "deviceModel": "iPhone 15 Pro",
    "operatingSystem": "iOS",
    "osVersion": "17.1",
    "mobileCarrier": "Verizon",
    "appCategory": "Social Media",
    "conModelScore": 750,
    "conCurrentDecision": "APPROVED"
  },
  "state": "CA"
}
```

**Partition Key**: `fields.state` (50 US states + DC)

## ⚡ Performance Tuning

### Auto-Scaling Configuration
The notebooks automatically adjust partitions and batch sizes based on record count:

| Record Count | Partitions | MongoDB Batch Size |
|-------------|------------|-------------------|
| ≤ 20M       | 200        | 2,000            |
| ≤ 50M       | 500        | 5,000            |
| ≤ 100M      | 800        | 8,000            |
| 200M+       | 1,000      | 10,000           |

### Optimization Tips
1. **Partition by state** - Aligns with MongoDB sharding strategy
2. **Use Delta Lake** - Provides ACID transactions and efficient reads
3. **Batch sizing** - Larger batches improve throughput but increase memory usage
4. **Upsert operations** - Prevents duplicates using `_id` and `fields.state`

## 🔧 Troubleshooting

### Fabric Environment Issues

**MongoDB Connection Errors**
- Verify Spark Java options are configured in custom environment
- Check MongoDB Spark Connector JAR is uploaded with success status
- Ensure connection string format is correct

**Performance Issues**
- Adjust `NUM_PARTITIONS` in data generator
- Increase batch sizes in insertion notebook for larger datasets
- Verify Delta Lake path is accessible

### Local Environment Issues

**mongosh Not Found**
- Install MongoDB Shell: https://www.mongodb.com/docs/mongodb-shell/install/
- Ensure mongosh is in your system PATH

**Connection Errors**
- Verify `.env` file exists with correct connection string
- Test connection using: `mongosh "your-connection-string"`
- Check firewall rules allow connections to DocumentDB/MongoDB

**Module Import Errors**
- Install dependencies: `pip install -r requirements.txt`

## 📊 Generate Explain Output

To generate MongoDB explain plans with `allPlansExecution` for all queries:

```bash
python 05_generate_explain_output.py
```

This will:
- Read queries from `data/mongodb_queries.json`
- Execute `.explain("allPlansExecution")` for each query
- Save output to `data/explain_out_<epoch>.txt`
- Include query descriptions, original queries, and full explain plans

Useful for sharing detailed execution plans with engineering teams.

## 🎨 Customization Pattern

To adapt this for your own data structure:

1. **Provide your sample JSON document** - Represents your target schema
2. **Describe your data requirements** - Record count, partitioning strategy, unique constraints
3. **Use GitHub Copilot** - Describe your schema and requirements, let Copilot generate the data generator
4. **Follow the separation pattern**:
   - Data generation → Delta Lake
   - Data insertion → MongoDB
   - Analysis → Performance testing

## 📚 References

- [Azure DocumentDB](https://learn.microsoft.com/azure/documentdb/) (formerly Azure Cosmos DB for MongoDB vCore)
- [MongoDB Spark Connector Documentation](https://www.mongodb.com/docs/spark-connector/)
- [Microsoft Fabric Spark Configuration](https://learn.microsoft.com/fabric/data-engineering/spark-compute)
- [Delta Lake Format](https://delta.io/)

## 📝 License

This is a reference implementation for performance testing. Adapt as needed for your specific use cases.

---

**Author**: Performance Testing Framework  
**Last Updated**: January 2026  
**Version**: 1.0
