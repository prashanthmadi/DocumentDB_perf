"""
Create MongoDB indexes from JSON file.

Usage: python 03_create_mongodb_indexes.py
"""

import subprocess
import sys
import json
import time
import os
import tempfile
from dotenv import load_dotenv


def load_config():
    load_dotenv()
    return {
        'connection_string': os.getenv('MONGODB_CONNECTION_STRING', ''),
        'database': os.getenv('MONGODB_DATABASE', 'mobile_apps'),
        'collection': os.getenv('MONGODB_COLLECTION', 'applications'),
        'indexes_file': os.getenv('INDEXES_FILE', 'data/mongodb_indexes.json'),
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '60'))
    }


def create_indexes(config):
    print(f"🔧 Reading indexes from {config['indexes_file']}...")
    
    with open(config['indexes_file'], 'r') as f:
        indexes = json.load(f)
    
    print(f"📋 Found {len(indexes)} indexes to create\n")
    
    results = []
    
    for idx in indexes:
        index_name = idx['name']
        index_keys = json.dumps(idx['keys'])
        
        print(f"🔍 Creating: {index_name}")
        start_time = time.time()
        
        try:
            js_script = f"""
            var targetDb = db.getSiblingDB('{config['database']}');
            var result = targetDb.{config['collection']}.createIndex({index_keys}, {{name: "{index_name}", background: true}});
            print(JSON.stringify(result));
            """
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                f.write(js_script)
                script_file = f.name
            
            result = subprocess.run(
                ['mongosh', config['connection_string'], '--quiet', '--file', script_file],
                capture_output=True, text=True, timeout=config['timeout']
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                print(f"   ✅ Created ({execution_time:.2f}s)")
                status = "SUCCESS"
            else:
                print(f"   ❌ Failed")
                status = "ERROR"
            
            results.append({
                'name': index_name,
                'status': status,
                'time': round(execution_time, 2)
            })
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({
                'name': index_name,
                'status': "ERROR",
                'time': round(time.time() - start_time, 2)
            })
        finally:
            try:
                os.unlink(script_file)
            except:
                pass
    
    successful = len([r for r in results if r['status'] == 'SUCCESS'])
    print(f"\n📊 Summary: {successful}/{len(results)} indexes created")
    
    return True


def main():
    print("=" * 60)
    print("MongoDB Index Creator")
    print("=" * 60 + "\n")
    
    config = load_config()
    print(f"Database: {config['database']}")
    print(f"Collection: {config['collection']}\n")
    
    if not os.path.exists(config['indexes_file']):
        print(f"❌ {config['indexes_file']} not found")
        return 1
    
    create_indexes(config)
    
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
