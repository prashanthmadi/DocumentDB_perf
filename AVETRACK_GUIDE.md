# Avetrack Performance Testing Guide - Azure DocumentDB

## Overview

Performance testing framework for avetrack data in **Azure DocumentDB** (MongoDB vCore). This POC tests a 4TB collection (~37M documents @ 116KB each) with queries based on actual customer patterns from queries.txt.

**Recommended Partition Key:** `fields.run`

## Query Pattern Analysis

### Primary Access Patterns

Almost every query follows this pattern:
```javascript
{
  "$and": [
    { "lastUpdateDateTime": "@DATE@" },              // Date range filter
    { "$or": [                                        // Run type filter
        { "fields.run": "RUN2" },
        { "fields.run": "MITIGATIONRESULT" }
    ]},
    // ... additional filters
  ]
}
```

### Query Pattern Frequency (from queries.txt)

- **100%** filter by `lastUpdateDateTime` (date ranges)
- **95%** filter by `fields.run` (RUN2, RUN3, RUN4, MITIGATIONRESULT)
- **90%** filter by `fields.checkOutList.orderList.casApplicationId`
- **85%** filter by `fields.checkOutList.orderList.itemDetailList.orderType`
- **60%** filter by `fields.common.ban` or `fields.tGuard.wirelessBAN`
- **40%** filter by `fields.authenticId.*` fields

## Partition Key Strategy

Using `fields.run` as partition key because:
- **95% of queries** filter by run type (RUN2, RUN3, RUN4, MITIGATIONRESULT)
- Natural data distribution across 4-6 run types
- Simple and effective for Azure DocumentDB

## Sample Queries

### Q1: RUN2 ELIGIBLE with BAN (Most Common)

```javascript
db.retailApplication.find({
  "$and": [
    { "lastUpdateDateTime": { $gte: ISODate('2024-01-01'), $lt: ISODate('2024-01-31') } },
    { "$or": [
        { "fields.run": "RUN2" },
        { "fields.run": "MITIGATIONRESULT" }
    ]},
    { "fields.checkOutList.orderList.itemDetailList.orderType": {
        $in: ["New", "Add-Line", "NEW", "ADD-LINE"]
    }},
    { "fields.checkOutList.orderList.casApplicationId": { $exists: true, $gt: "0" } },
    { "fields.common.ban": "337201967167" }
  ]
}).limit(100)
```

### Q2: RUN2 ELIGIBLE with AuthenticID

```javascript
db.retailApplication.find({
  "$and": [
    { "lastUpdateDateTime": { $gte: ISODate('2024-01-01') } },
    { "$or": [
        { "fields.run": "RUN2" },
        { "fields.run": "MITIGATIONRESULT" }
    ]},
    { "fields.checkOutList.orderList.itemDetailList.orderType": {
        $in: ["New", "Add-Line"]
    }},
    { "fields.checkOutList.orderList.casApplicationId": { $exists: true, $gt: "0" } },
    { "fields.authenticId.TransactionId": { $exists: true } },
    { "fields.authenticId.TransactionDate": { $exists: true } },
    { "fields.authenticId.DocumentNumber": { $exists: true } }
  ]
}).limit(100)
```

### Q3: CARE RUN2 BYOP with IMEI

```javascript
db.careApplication.find({
  "$and": [
    { "lastUpdateDateTime": { $gte: ISODate('2024-01-01') } },
    { "$or": [
        { "fields.run": "RUN2" },
        { "fields.run": "MITIGATIONRESULT" }
    ]},
    { "fields.checkOutList.orderList.itemDetailList.orderType": { $in: ["New", "NEW"] } },
    { "fields.checkOutList.orderList.casApplicationId": { $exists: true, $gt: "0" } },
    { "fields.checkOutList.orderList.itemDetailList.transactionType": "BYOP" },
    { "fields.checkOutList.orderList.itemDetailList.imei": { $exists: true, $gt: "" } }
  ]
}).limit(100)
```

### Q4: Care Model Cases by Rule Numbers

```javascript
db.msAvertackCareCase.find({
  "$and": [
    { "createdDateTime": { $gte: ISODate('2024-01-01') } },
    { "rules": { $in: ["212", "230"] } },
    { "caseApplicationRecordList.applicationRecord.fields.checkOutList.orderList.casApplicationId": {
        $exists: true
    }},
    { "gfmsReferralStr": { $gt: "" } }
  ]
}).limit(100)
```

### Q5: CARE Fraud Alerts

```javascript
db.careApplication.find({
  "$and": [
    { "lastUpdateDateTime": { $gte: ISODate('2024-01-01') } },
    { "fields.fraudAlert.isFraud": true }
  ]
}).limit(100)
```

### Q6: Digital Hotlist Rules

```javascript
db.msAvertackDigitalCase.find({
  "$and": [
    { "createdDateTime": { $gte: ISODate('2024-01-01') } },
    { "rules": { $in: ["78"] } },
    { "mitigationStr": { $regex: ".*40[89].*" } }
  ]
}).limit(100)
```

## Performance Metrics

Track these metrics for each query:
- **Query Execution Time** - Average, P50, P95, P99 response times (milliseconds)
- **Throughput** - Queries per second (QPS)
- **Documents Scanned** - Scanned vs returned ratio (index efficiency)
- **Index Usage** - Which indexes were used
- **CPU & Memory Usage** - vCore resource utilization

## Implementation Steps

### 1. Generate Test Data (116KB documents)
```bash
# Open and run: 01_avetrack_data_generator_pyspark.ipynb
# Configuration: NUM_RECORDS = 1000000 (start with 1M for testing)
# Output: Delta Lake table partitioned by fields.run
```

### 2. Create DocumentDB Collection
```javascript
// Create collection with partition key
db.createCollection("avetrack_applications", {
  shardKey: { "run": "hashed" }
})
```

### 3. Insert Data into DocumentDB
```bash
# Use your preferred method to insert from Delta Lake to DocumentDB
# - Azure Data Factory
# - Custom Python script
# - Spark MongoDB connector
```

### 4. Create Indexes
```bash
# Load and execute indexes from: data/mongodb_avetrack_indexes.json
# Use 03_create_mongodb_indexes.py or similar script
```

### 5. Run Performance Tests
```bash
# Execute queries from: data/mongodb_avetrack_queries.json
# Use 04_query_performance_server_time.py
# Measure query execution time and throughput
```

### 6. Monitor and Optimize
- Monitor query execution times
- Identify slow queries and optimize indexes
- Adjust indexes as needed
- Scale vCores based on workload (CPU/memory usage)

## Files Reference

| File | Purpose |
|------|---------|
| `01_avetrack_data_generator_pyspark.ipynb` | Generate 116KB documents |
| `data/mongodb_avetrack_indexes.json` | 30 indexes for performance |
| `data/mongodb_avetrack_queries.json` | 35 test queries from customer patterns |
| `data/queries.txt` | Original 4,174 lines of customer queries |
