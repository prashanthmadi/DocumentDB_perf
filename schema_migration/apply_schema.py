"""
Apply MongoDB Schema to Destination Server

Reads schema JSON and creates databases, collections, indexes, and shard keys
on destination MongoDB/DocumentDB server.

Usage:
    python apply_schema.py --schema schema.json
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
    connection_string = os.getenv('DEST_MONGODB_CONNECTION_STRING', '')
    db_prefix = os.getenv('DATABASE_PREFIX', '')
    
    if not connection_string:
        print("❌ Error: DEST_MONGODB_CONNECTION_STRING not found in .env file")
        print("\nSteps to fix:")
        print("   1. Copy .env.template to .env")
        print("   2. Update DEST_MONGODB_CONNECTION_STRING with your destination endpoint\n")
        sys.exit(1)
    
    return {
        'connection_string': connection_string,
        'db_prefix': db_prefix,
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '120'))
    }


def load_schema(schema_file: str) -> dict:
    """Load schema from JSON file"""
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        print(f"✅ Loaded schema: {schema_file}")
        return schema
    except Exception as e:
        print(f"❌ Error loading schema: {e}")
        sys.exit(1)


def generate_apply_script(schema: dict, db_prefix: str) -> str:
    """Generate mongosh script to apply schema to destination"""
    
    databases = schema.get('databases', [])
    
    script_lines = [
        "// Auto-generated schema creation script",
        "// Apply complete MongoDB schema to destination",
        "",
        "print('============================================================');",
        "print('Applying Schema to Destination MongoDB');",
        "print('============================================================');",
        f"print('Total Databases: {len(databases)}');",
        f"print('Database Prefix: {db_prefix if db_prefix else 'None'}');",
        "print('============================================================');",
        "print('');",
        "",
        "var results = { databases: 0, collections: 0, indexes: 0, errors: [] };",
        ""
    ]
    
    for db_info in databases:
        db_name = db_info['database']
        target_db_name = f"{db_prefix}{db_name}" if db_prefix else db_name
        
        script_lines.extend([
            f"// Database: {target_db_name}",
            f"print('\\n📁 Database: {target_db_name}');",
            f"var targetDb = db.getSiblingDB('{target_db_name}');",
            ""
        ])
        
        # Check if any collection is sharded
        has_sharded = any(coll.get('is_sharded', False) for coll in db_info['collections'])
        
        if has_sharded:
            script_lines.extend([
                f"// Enable sharding on database",
                "try {",
                f"    var adminDb = db.getSiblingDB('admin');",
                f"    adminDb.runCommand({{ enableSharding: '{target_db_name}' }});",
                f"    print('   ✅ Sharding enabled on database');",
                "} catch(e) {",
                "    // May already be enabled or not supported",
                f"    print('   ⚠️  Could not enable sharding: ' + e.message);",
                "}",
                ""
            ])
        
        for coll in db_info['collections']:
            coll_name = coll['name']
            
            script_lines.extend([
                f"// Collection: {coll_name}",
                f"print('   📄 Creating collection: {coll_name}');",
                ""
            ])
            
            # Create collection
            if coll.get('is_sharded'):
                shard_key_json = json.dumps(coll['shard_key'])
                script_lines.extend([
                    f"// Shard collection with key: {shard_key_json}",
                    "try {",
                    f"    targetDb.createCollection('{coll_name}');",
                    f"    var adminDb = db.getSiblingDB('admin');",
                    f"    adminDb.runCommand({{ shardCollection: '{target_db_name}.{coll_name}', key: {shard_key_json} }});",
                    f"    print('      ✅ Collection created and sharded');",
                    "    results.collections++;",
                    "} catch(e) {",
                    f"    print('      ❌ Error: ' + e.message);",
                    f"    results.errors.push({{ db: '{target_db_name}', collection: '{coll_name}', error: e.message }});",
                    "}",
                    ""
                ])
            else:
                script_lines.extend([
                    "try {",
                    f"    targetDb.createCollection('{coll_name}');",
                    f"    print('      ✅ Collection created');",
                    "    results.collections++;",
                    "} catch(e) {",
                    f"    print('      ❌ Error: ' + e.message);",
                    f"    results.errors.push({{ db: '{target_db_name}', collection: '{coll_name}', error: e.message }});",
                    "}",
                    ""
                ])
            
            # Create indexes
            if coll.get('indexes'):
                script_lines.append(f"   // Creating {len(coll['indexes'])} indexes")
                
                for idx in coll['indexes']:
                    # Skip _id index
                    if idx['name'] == '_id_':
                        continue
                    
                    idx_name = idx['name']
                    idx_keys = json.dumps(idx['keys'])
                    
                    # Build index options
                    options = [f"name: '{idx_name}'"]
                    if idx.get('unique'):
                        options.append("unique: true")
                    if idx.get('sparse'):
                        options.append("sparse: true")
                    if idx.get('background'):
                        options.append("background: true")
                    if idx.get('expireAfterSeconds') is not None:
                        options.append(f"expireAfterSeconds: {idx['expireAfterSeconds']}")
                    
                    options_str = ", ".join(options)
                    
                    script_lines.extend([
                        "   try {",
                        f"       targetDb.{coll_name}.createIndex({idx_keys}, {{ {options_str} }});",
                        f"       print('      ✅ Index: {idx_name}');",
                        "       results.indexes++;",
                        "   } catch(e) {",
                        f"       print('      ❌ Index {idx_name} failed: ' + e.message);",
                        f"       results.errors.push({{ db: '{target_db_name}', collection: '{coll_name}', index: '{idx_name}', error: e.message }});",
                        "   }",
                        ""
                    ])
                
                script_lines.append("")
        
        script_lines.extend([
            "results.databases++;",
            ""
        ])
    
    script_lines.extend([
        "print('');",
        "print('============================================================');",
        "print('Schema Application Complete');",
        "print('============================================================');",
        "print('Databases Created: ' + results.databases);",
        "print('Collections Created: ' + results.collections);",
        "print('Indexes Created: ' + results.indexes);",
        "if (results.errors.length > 0) {",
        "    print('Errors: ' + results.errors.length);",
        "    print('');",
        "    print('Error Details:');",
        "    results.errors.forEach(function(err) {",
        "        print('  - ' + JSON.stringify(err));",
        "    });",
        "}",
        "print('============================================================');",
        ""
    ])
    
    return "\n".join(script_lines)


def apply_schema(config: dict, script: str):
    """Execute mongosh script to apply schema"""
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
            print(result.stdout)
            return True
        else:
            print(f"❌ Error applying schema:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ Script execution timed out after {config['timeout']} seconds")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Apply MongoDB schema to destination server')
    parser.add_argument('--schema', default='schema.json', help='Input schema JSON file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    # Load schema
    schema = load_schema(args.schema)
    
    # Summary
    databases = schema.get('databases', [])
    total_collections = sum(len(db['collections']) for db in databases)
    total_indexes = sum(sum(len(coll['indexes']) for coll in db['collections']) for db in databases)
    sharded_collections = sum(sum(1 for coll in db['collections'] if coll.get('is_sharded')) for db in databases)
    
    print(f"\n📊 Schema Summary:")
    print(f"   Databases: {len(databases)}")
    print(f"   Collections: {total_collections}")
    print(f"   Indexes: {total_indexes}")
    print(f"   Sharded Collections: {sharded_collections}")
    
    for db in databases:
        print(f"\n   📁 {db['database']}")
        for coll in db['collections']:
            shard_info = " [SHARDED]" if coll.get('is_sharded') else ""
            print(f"      - {coll['name']} ({len(coll['indexes'])} indexes){shard_info}")
    
    print(f"\n🔌 Connecting to destination and applying schema...\n")
    
    # Generate script
    script = generate_apply_script(schema, config['db_prefix'])
    
    # Apply schema
    success = apply_schema(config, script)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
