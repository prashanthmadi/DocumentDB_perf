"""
Apply MongoDB Schema to Destination Server (Using Python SDK)

Reads schema JSON and creates databases, collections, indexes, and shard keys
on destination MongoDB/DocumentDB server using pymongo library.

Usage:
    python apply_schema_noncli.py --schema schema.json

Requirements:
    pip install pymongo python-dotenv
"""

import json
import sys
import os
from typing import Dict, List
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import (
    ConnectionFailure, 
    OperationFailure, 
    ServerSelectionTimeoutError,
    ConfigurationError
)


def load_config() -> Dict:
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
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '120')) * 1000  # Convert to milliseconds
    }


def load_schema(schema_file: str) -> Dict:
    """Load schema from JSON file"""
    try:
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        print(f"✅ Loaded schema: {schema_file}")
        return schema
    except FileNotFoundError:
        print(f"❌ Error: Schema file not found: {schema_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing schema JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading schema: {e}")
        sys.exit(1)


def mask_password(connection_string: str) -> str:
    """Mask password in connection string for logging"""
    try:
        if '@' in connection_string:
            parts = connection_string.split('@')
            if len(parts) >= 2 and '://' in parts[0]:
                protocol_and_creds = parts[0]
                if ':' in protocol_and_creds.split('//')[-1]:
                    protocol_user = protocol_and_creds.rsplit(':', 1)[0]
                    return f"{protocol_user}:***@{parts[1]}"
        return connection_string
    except:
        return "***"


def apply_schema(config: Dict, schema: Dict) -> bool:
    """Apply schema to destination MongoDB/DocumentDB using pymongo"""
    
    databases = schema.get('databases', [])
    results = {
        'databases': 0,
        'collections': 0,
        'indexes': 0,
        'errors': []
    }
    
    print("\n============================================================")
    print("Applying Schema to Destination MongoDB")
    print("============================================================")
    print(f"Total Databases: {len(databases)}")
    print(f"Database Prefix: {config['db_prefix'] if config['db_prefix'] else 'None'}")
    print(f"Connection: {mask_password(config['connection_string'])}")
    print("============================================================")
    print("")
    
    client = None
    
    try:
        # Connect to MongoDB
        print("🔌 Connecting to destination server...")
        client = MongoClient(
            config['connection_string'],
            serverSelectionTimeoutMS=config['timeout'],
            connectTimeoutMS=config['timeout'],
            socketTimeoutMS=config['timeout']
        )
        
        # Test connection
        client.admin.command('ping')
        print("✅ Connected successfully\n")
        
        # Process each database
        for db_info in databases:
            db_name = db_info['database']
            target_db_name = f"{config['db_prefix']}{db_name}" if config['db_prefix'] else db_name
            
            print(f"📁 Database: {target_db_name}")
            
            target_db = client[target_db_name]
            
            # Check if any collection is sharded
            has_sharded = any(coll.get('is_sharded', False) for coll in db_info['collections'])
            
            if has_sharded:
                # Try to enable sharding on database
                try:
                    client.admin.command('enableSharding', target_db_name)
                    print(f"   ✅ Sharding enabled on database")
                except OperationFailure as e:
                    # May already be enabled or not supported (e.g., DocumentDB)
                    print(f"   ⚠️  Could not enable sharding: {e}")
                except Exception as e:
                    print(f"   ⚠️  Could not enable sharding: {e}")
            
            # Process each collection
            for coll_info in db_info['collections']:
                coll_name = coll_info['name']
                print(f"   📄 Creating collection: {coll_name}")
                
                try:
                    # Create collection
                    if coll_info.get('is_sharded'):
                        # For sharded collections, create and then shard
                        shard_key = coll_info['shard_key']
                        
                        try:
                            # Create collection first
                            target_db.create_collection(coll_name)
                            
                            # Try to shard it
                            try:
                                client.admin.command(
                                    'shardCollection',
                                    f"{target_db_name}.{coll_name}",
                                    key=shard_key
                                )
                                print(f"      ✅ Collection created and sharded")
                            except OperationFailure as e:
                                print(f"      ⚠️  Collection created but sharding failed: {e}")
                                print(f"      ℹ️  This is expected for Azure Cosmos DB (DocumentDB API)")
                        except OperationFailure as e:
                            if 'already exists' in str(e).lower():
                                print(f"      ℹ️  Collection already exists")
                            else:
                                raise
                    else:
                        # Regular (non-sharded) collection
                        try:
                            target_db.create_collection(coll_name)
                            print(f"      ✅ Collection created")
                        except OperationFailure as e:
                            if 'already exists' in str(e).lower():
                                print(f"      ℹ️  Collection already exists")
                            else:
                                raise
                    
                    results['collections'] += 1
                    
                except Exception as e:
                    error_msg = f"Error creating collection: {e}"
                    print(f"      ❌ {error_msg}")
                    results['errors'].append({
                        'db': target_db_name,
                        'collection': coll_name,
                        'error': str(e)
                    })
                    continue
                
                # Create indexes
                indexes = coll_info.get('indexes', [])
                if indexes:
                    print(f"   // Creating {len(indexes)} indexes")
                    
                    collection = target_db[coll_name]
                    
                    for idx in indexes:
                        # Skip _id index (automatically created)
                        if idx['name'] == '_id_':
                            continue
                        
                        idx_name = idx['name']
                        idx_keys = idx['keys']
                        
                        try:
                            # Build index options
                            index_options = {'name': idx_name}
                            
                            if idx.get('unique'):
                                index_options['unique'] = True
                            if idx.get('sparse'):
                                index_options['sparse'] = True
                            if idx.get('background'):
                                index_options['background'] = True
                            
                            # Convert index keys to list of tuples
                            # MongoDB expects [(field, direction), ...]
                            # Handle string numeric values from JSON
                            index_spec = []
                            for field, direction in idx_keys.items():
                                # Convert string numbers to int (e.g., "1" -> 1, "-1" -> -1)
                                if isinstance(direction, str):
                                    if direction in ['1', '-1']:
                                        direction = int(direction)
                                    # Otherwise keep as string (e.g., 'text', '2d', 'hashed', '2dsphere')
                                elif isinstance(direction, (int, float)):
                                    direction = int(direction)
                                index_spec.append((field, direction))
                            
                            # TTL indexes (expireAfterSeconds) only work on single-field indexes
                            # Check if this is a compound index with TTL
                            if idx.get('expireAfterSeconds') is not None:
                                if len(index_spec) > 1:
                                    print(f"      ⚠️  Index {idx_name}: TTL (expireAfterSeconds) only works on single-field indexes, skipping TTL on compound index")
                                else:
                                    index_options['expireAfterSeconds'] = idx['expireAfterSeconds']
                            
                            # Create index
                            collection.create_index(index_spec, **index_options)
                            print(f"      ✅ Index: {idx_name}")
                            results['indexes'] += 1
                            
                        except OperationFailure as e:
                            error_msg = str(e)
                            if 'already exists' in error_msg.lower():
                                print(f"      ℹ️  Index {idx_name} already exists")
                                results['indexes'] += 1
                            else:
                                print(f"      ❌ Index {idx_name} failed: {error_msg}")
                                results['errors'].append({
                                    'db': target_db_name,
                                    'collection': coll_name,
                                    'index': idx_name,
                                    'error': error_msg
                                })
                        except Exception as e:
                            print(f"      ❌ Index {idx_name} failed: {e}")
                            results['errors'].append({
                                'db': target_db_name,
                                'collection': coll_name,
                                'index': idx_name,
                                'error': str(e)
                            })
                
                print("")
            
            results['databases'] += 1
        
        # Print summary
        print("")
        print("============================================================")
        print("Schema Application Complete")
        print("============================================================")
        print(f"Databases Created: {results['databases']}")
        print(f"Collections Created: {results['collections']}")
        print(f"Indexes Created: {results['indexes']}")
        
        if results['errors']:
            print(f"Errors: {len(results['errors'])}")
            print("")
            print("Error Details:")
            for err in results['errors']:
                print(f"  - {json.dumps(err)}")
        
        print("============================================================")
        
        return len(results['errors']) == 0
        
    except ServerSelectionTimeoutError:
        print(f"\n❌ Connection timeout: Could not connect to MongoDB server")
        print(f"   Connection string: {mask_password(config['connection_string'])}")
        print(f"   Timeout: {config['timeout']/1000}s")
        print("\n💡 Troubleshooting:")
        print("   - Verify the connection string is correct")
        print("   - Check firewall rules and network connectivity")
        print("   - Ensure the server is running and accessible")
        return False
        
    except ConnectionFailure as e:
        print(f"\n❌ Connection failed: {e}")
        print(f"   Connection string: {mask_password(config['connection_string'])}")
        print("\n💡 Troubleshooting:")
        print("   - Verify your credentials are correct")
        print("   - Check if the server requires TLS/SSL")
        print("   - Ensure the server is running")
        return False
        
    except ConfigurationError as e:
        print(f"\n❌ Configuration error: {e}")
        print(f"   Connection string: {mask_password(config['connection_string'])}")
        print("\n💡 Check your connection string format")
        return False
        
    except OperationFailure as e:
        print(f"\n❌ Authentication or operation failed: {e}")
        print("\n💡 Troubleshooting:")
        print("   - Verify your username and password")
        print("   - Check authentication database (authSource parameter)")
        print("   - Ensure your user has necessary permissions")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        print("\n--- Stack Trace ---")
        traceback.print_exc()
        print("--- End of Stack Trace ---")
        return False
        
    finally:
        if client:
            client.close()
            print("\n🔌 Connection closed")


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Apply MongoDB schema to destination server using Python SDK'
    )
    parser.add_argument(
        '--schema', 
        default='schema.json', 
        help='Input schema JSON file (default: schema.json)'
    )
    args = parser.parse_args()
    
    print("="*60)
    print("MongoDB Schema Application Tool (Python SDK)")
    print("="*60)
    print()
    
    # Load configuration
    config = load_config()
    
    # Load schema
    schema = load_schema(args.schema)
    
    # Summary
    databases = schema.get('databases', [])
    total_collections = sum(len(db['collections']) for db in databases)
    total_indexes = sum(
        sum(len(coll['indexes']) for coll in db['collections']) 
        for db in databases
    )
    sharded_collections = sum(
        sum(1 for coll in db['collections'] if coll.get('is_sharded')) 
        for db in databases
    )
    
    print(f"📊 Schema Summary:")
    print(f"   Databases: {len(databases)}")
    print(f"   Collections: {total_collections}")
    print(f"   Indexes: {total_indexes}")
    print(f"   Sharded Collections: {sharded_collections}")
    
    for db in databases:
        print(f"\n   📁 {db['database']}")
        for coll in db['collections']:
            shard_info = " [SHARDED]" if coll.get('is_sharded') else ""
            idx_count = len(coll.get('indexes', []))
            print(f"      - {coll['name']} ({idx_count} indexes){shard_info}")
    
    # Apply schema
    success = apply_schema(config, schema)
    
    if not success:
        sys.exit(1)
    else:
        print("\n✅ Schema application completed successfully!")


if __name__ == "__main__":
    main()
