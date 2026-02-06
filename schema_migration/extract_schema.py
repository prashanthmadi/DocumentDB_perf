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


def mask_password(connection_string: str) -> str:
    """Mask password in connection string for logging"""
    try:
        if '@' in connection_string:
            # Format: mongodb://user:password@host...
            parts = connection_string.split('@')
            if len(parts) >= 2 and '://' in parts[0]:
                protocol_and_creds = parts[0]
                if ':' in protocol_and_creds.split('//')[-1]:
                    # Has user:password
                    protocol_user = protocol_and_creds.rsplit(':', 1)[0]
                    return f"{protocol_user}:***@{parts[1]}"
        return connection_string
    except:
        return "***"


def check_mongo_cli(force_legacy: bool = False) -> str:
    """Check which MongoDB CLI is available (mongosh or mongo)"""
    print("🔍 Checking for MongoDB CLI...")
    
    if force_legacy:
        print("   ⚠️  Forcing legacy 'mongo' CLI (--use-legacy-cli flag)")
        # Only try mongo
        try:
            result = subprocess.run(['mongo', '--version'], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                version = result.stdout.strip().split('\n')[0]
                print(f"   ✓ Found mongo: {version}")
                return 'mongo'
        except FileNotFoundError:
            print("   ✗ mongo not found")
        except Exception as e:
            print(f"   ✗ Error checking mongo: {e}")
        
        print("\n❌ Error: Legacy 'mongo' CLI not found")
        print("\nPlease install MongoDB 3.6 or 4.0 client tools\n")
        sys.exit(1)
    
    # Try mongosh first (newer)
    try:
        result = subprocess.run(['mongosh', '--version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version = result.stdout.strip().split('\n')[0]
            print(f"   ✓ Found mongosh: {version}")
            return 'mongosh'
    except FileNotFoundError:
        print("   ✗ mongosh not found")
    except Exception as e:
        print(f"   ✗ Error checking mongosh: {e}")
    
    # Try mongo (legacy)
    try:
        result = subprocess.run(['mongo', '--version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version = result.stdout.strip().split('\n')[0]
            print(f"   ✓ Found mongo: {version}")
            return 'mongo'
    except FileNotFoundError:
        print("   ✗ mongo not found")
    except Exception as e:
        print(f"   ✗ Error checking mongo: {e}")
    
    print("\n❌ Error: No MongoDB CLI found (neither 'mongosh' nor 'mongo')")
    print("\nPlease install MongoDB Shell:")
    print("   - For MongoDB 4.0+: https://www.mongodb.com/try/download/shell")
    print("   - For MongoDB 3.6: Install MongoDB which includes the 'mongo' CLI\n")
    sys.exit(1)


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
    
    print(f"📝 Connection string loaded: {mask_password(connection_string)}")
    
    return {
        'connection_string': connection_string,
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '120'))
    }


def extract_schema(config: dict, mongo_cli: str) -> dict:
    """Extract complete schema from source MongoDB"""
    
    print(f"🔧 Using MongoDB CLI: {mongo_cli}")
    print(f"⏱️  Timeout: {config['timeout']} seconds\n")
    
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
        print("📝 Creating temporary script file...")
        with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False, encoding='utf-8') as tmp:
            tmp.write(script)
            tmp_path = tmp.name
        print(f"   Script file: {tmp_path}")
        
        # Build command based on CLI version
        if mongo_cli == 'mongosh':
            cmd = ['mongosh', config['connection_string'], '--file', tmp_path, '--quiet']
        else:
            # For legacy mongo CLI
            cmd = ['mongo', config['connection_string'], '--quiet', tmp_path]
        
        print(f"\n🚀 Executing command...")
        print(f"   Command: {cmd[0]} {mask_password(' '.join(cmd[1:]))}")
        print(f"   Working directory: {os.getcwd()}")
        print(f"\n⏳ Connecting to MongoDB server...\n")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=config['timeout']
        )
        
        # Clean up temp file
        try:
            os.unlink(tmp_path)
            print("🧹 Cleaned up temporary script file")
        except:
            pass
        
        print(f"\n📊 Command completed with exit code: {result.returncode}\n")
        
        if result.returncode == 0:
            print("✓ Connection successful")
            print(f"📄 Output length: {len(result.stdout)} characters")
            
            if result.stdout.strip():
                print("\n🔍 Parsing JSON output...")
                try:
                    # Parse JSON output
                    schema = json.loads(result.stdout)
                    print("✓ JSON parsed successfully\n")
                    return schema
                except json.JSONDecodeError as e:
                    print(f"\n❌ Error parsing schema JSON: {e}")
                    print(f"\n--- Raw Output (first 1000 chars) ---")
                    print(result.stdout[:1000])
                    print("\n--- End of Output ---\n")
                    sys.exit(1)
            else:
                print("\n❌ Error: No output received from MongoDB")
                if result.stderr:
                    print(f"\nStderr: {result.stderr}")
                sys.exit(1)
        else:
            print("\n❌ Error extracting schema:")
            print(f"\nExit code: {result.returncode}")
            
            if result.stderr:
                print(f"\n--- Error Output ---")
                print(result.stderr)
                print("--- End of Error Output ---\n")
            
            if result.stdout:
                print(f"\n--- Standard Output ---")
                print(result.stdout)
                print("--- End of Standard Output ---\n")
            
            # Parse specific error types
            error_msg = result.stderr + result.stdout
            if 'wire version' in error_msg and 'requires at least' in error_msg:
                print("\n💡 MongoDB Version Incompatibility Detected:")
                print("   - Your MongoDB server is too old for mongosh")
                print("   - mongosh requires MongoDB 4.2+")
                print("   - Your server appears to be MongoDB 3.6 or earlier")
                print("\n   SOLUTION: Use the legacy 'mongo' CLI instead:")
                print("   1. Install MongoDB 3.6 client tools (includes 'mongo' CLI)")
                print("   2. Re-run this script - it will auto-detect the 'mongo' CLI")
                print("   3. Or force it: python extract_schema.py --use-legacy-cli\n")
            elif 'ENOTFOUND' in error_msg or 'getaddrinfo' in error_msg:
                print("\n💡 DNS Resolution Error Detected:")
                print("   - The hostname in your connection string cannot be resolved")
                print("   - Check that the hostname is correct")
                print("   - Verify network connectivity and DNS settings")
                print(f"   - Connection string: {mask_password(config['connection_string'])}\n")
            elif 'ECONNREFUSED' in error_msg:
                print("\n💡 Connection Refused:")
                print("   - The server is not accepting connections on the specified port")
                print("   - Verify the server is running and the port is correct\n")
            elif 'Authentication failed' in error_msg or 'auth failed' in error_msg:
                print("\n💡 Authentication Error:")
                print("   - Check your username and password")
                print("   - Verify the authentication database is correct\n")
            elif 'ETIMEDOUT' in error_msg or 'timed out' in error_msg:
                print("\n💡 Connection Timeout:")
                print("   - The server is not responding")
                print("   - Check firewall rules and network connectivity\n")
            
            sys.exit(1)
            
    except subprocess.TimeoutExpired:
        print(f"\n❌ Schema extraction timed out after {config['timeout']} seconds")
        print("\n💡 Suggestions:")
        print("   - Increase timeout in .env file: TIMEOUT_SECONDS=300")
        print("   - Check network connectivity to MongoDB server")
        print("   - Verify the server is responding\n")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"\n❌ Command not found: {e}")
        print(f"\n💡 Make sure {mongo_cli} is installed and available in your PATH\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        print("\n--- Stack Trace ---")
        traceback.print_exc()
        print("--- End of Stack Trace ---\n")
        sys.exit(1)


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract MongoDB schema from source server')
    parser.add_argument('--output', default='schema.json', help='Output schema JSON file')
    parser.add_argument('--use-legacy-cli', action='store_true', 
                       help='Force use of legacy "mongo" CLI instead of "mongosh" (for MongoDB 3.6)')
    args = parser.parse_args()
    
    print("="*60)
    print("MongoDB Schema Extraction Tool")
    print("="*60)
    print()
    
    # Check MongoDB CLI availability
    mongo_cli = check_mongo_cli(force_legacy=args.use_legacy_cli)
    print()
    
    print("🔌 Connecting to source MongoDB server...")
    
    # Load configuration
    config = load_config()
    print()
    
    # Extract schema
    print("📊 Extracting schema (databases, collections, indexes, shard keys)...")
    schema = extract_schema(config, mongo_cli)
    
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
