"""
Extract MongoDB Schema from Source Server

Connects to source MongoDB server and extracts complete schema including:
- Databases
- Collections
- Indexes
- Shard keys
- Collection options

Usage:
    python extract_schema.py --output schema.json
"""

import json
import sys
import os
import subprocess
import tempfile
from dotenv import load_dotenv


def load_config():
    """Load configuration from .env file"""
    load_dotenv()
    connection_string = os.getenv('SOURCE_MONGODB_CONNECTION_STRING', '')
    
    if not connection_string:
        print("❌ Error: SOURCE_MONGODB_CONNECTION_STRING not found in .env file")
        print("\nSteps to fix:")
        print("   1. Copy .env.template to .env")
        print("   2. Update SOURCE_MONGODB_CONNECTION_STRING with your source MongoDB endpoint\n")
        sys.exit(1)
    
    return {
        'connection_string': connection_string,
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '120'))
    }


def extract_schema(config: dict) -> dict:
    """Extract complete schema from source MongoDB"""
    
    # MongoDB script to extract schema
    script = """
// Extract complete MongoDB schema
var schema = {
    extracted_at: new Date().toISOString(),
    databases: []
};

var adminDb = db.getSiblingDB('admin');
var dbList = adminDb.runCommand({ listDatabases: 1 }).databases;

// Filter out system databases
var userDatabases = dbList.filter(function(db) {
    return !['admin', 'local', 'config'].includes(db.name);
});

userDatabases.forEach(function(dbInfo) {
    var dbName = dbInfo.name;
    var currentDb = db.getSiblingDB(dbName);
    
    var dbSchema = {
        database: dbName,
        size_gb: dbInfo.sizeOnDisk ? (dbInfo.sizeOnDisk / (1024*1024*1024)) : 0,
        collections: []
    };
    
    // Get all collections
    var collections = currentDb.getCollectionNames();
    
    collections.forEach(function(collName) {
        var coll = currentDb.getCollection(collName);
        
        // Get collection stats
        var stats = {};
        try {
            stats = currentDb.runCommand({ collStats: collName });
        } catch(e) {
            stats = { count: 0, size: 0, avgObjSize: 0 };
        }
        
        // Get indexes
        var indexes = [];
        try {
            coll.getIndexes().forEach(function(idx) {
                indexes.push({
                    name: idx.name,
                    keys: idx.key,
                    unique: idx.unique || false,
                    sparse: idx.sparse || false,
                    background: idx.background || false,
                    expireAfterSeconds: idx.expireAfterSeconds
                });
            });
        } catch(e) {
            // Ignore index errors
        }
        
        // Check if collection is sharded
        var shardKey = null;
        var isSharded = false;
        try {
            var configDb = db.getSiblingDB('config');
            var shardInfo = configDb.collections.findOne({ _id: dbName + '.' + collName });
            if (shardInfo) {
                isSharded = true;
                shardKey = shardInfo.key;
            }
        } catch(e) {
            // Not sharded or no access to config
        }
        
        var collSchema = {
            name: collName,
            doc_count: stats.count || 0,
            size_gb: stats.size ? (stats.size / (1024*1024*1024)) : 0,
            avg_doc_size: stats.avgObjSize || 0,
            indexes: indexes,
            is_sharded: isSharded,
            shard_key: shardKey
        };
        
        dbSchema.collections.push(collSchema);
    });
    
    schema.databases.push(dbSchema);
});

// Output as JSON
print(JSON.stringify(schema, null, 2));
"""
    
    try:
        # Create temporary file for the script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False, encoding='utf-8') as tmp:
            tmp.write(script)
            tmp_path = tmp.name
        
        # Execute mongosh
        cmd = ['mongosh', config['connection_string'], '--file', tmp_path, '--quiet']
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=config['timeout']
        )
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        if result.returncode == 0:
            # Parse JSON output
            schema = json.loads(result.stdout)
            return schema
        else:
            print(f"❌ Error extracting schema:")
            print(result.stderr)
            sys.exit(1)
            
    except subprocess.TimeoutExpired:
        print(f"❌ Schema extraction timed out after {config['timeout']} seconds")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing schema JSON: {e}")
        print(f"Output: {result.stdout}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract MongoDB schema from source server')
    parser.add_argument('--output', default='schema.json', help='Output schema JSON file')
    args = parser.parse_args()
    
    print("🔌 Connecting to source MongoDB server...")
    
    # Load configuration
    config = load_config()
    
    # Extract schema
    print("📊 Extracting schema (databases, collections, indexes, shard keys)...\n")
    schema = extract_schema(config)
    
    # Calculate totals
    total_collections = sum(len(db['collections']) for db in schema['databases'])
    total_indexes = sum(sum(len(coll['indexes']) for coll in db['collections']) for db in schema['databases'])
    sharded_collections = sum(sum(1 for coll in db['collections'] if coll.get('is_sharded')) for db in schema['databases'])
    
    # Write to JSON file
    try:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        
        print(f"✅ Schema extracted and saved to: {args.output}")
        print(f"\n📊 Summary:")
        print(f"   Databases: {len(schema['databases'])}")
        print(f"   Collections: {total_collections}")
        print(f"   Indexes: {total_indexes}")
        print(f"   Sharded Collections: {sharded_collections}")
        
        for db in schema['databases']:
            print(f"\n   📁 {db['database']} ({db['size_gb']:.3f} GB)")
            print(f"      Collections: {len(db['collections'])}")
            for coll in db['collections']:
                shard_info = f" [SHARDED: {json.dumps(coll['shard_key'])}]" if coll.get('is_sharded') else ""
                print(f"         - {coll['name']} ({coll['doc_count']:,} docs, {len(coll['indexes'])} indexes){shard_info}")
        
        print(f"\n💡 Next step:")
        print(f"   1. Review/edit {args.output} to remove unwanted databases/collections")
        print(f"   2. Run: python apply_schema.py --schema {args.output}")
        
    except Exception as e:
        print(f"❌ Error writing schema file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
