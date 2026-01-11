"""
Execute MongoDB queries from JSON file and measure performance.

Usage: python 04_run_mongodb_queries.py
"""

import subprocess
import sys
import json
import os
import csv
import tempfile
from datetime import datetime
from dotenv import load_dotenv


def load_config():
    load_dotenv()
    return {
        'connection_string': os.getenv('MONGODB_CONNECTION_STRING', ''),
        'database': os.getenv('MONGODB_DATABASE', 'mobile_apps'),
        'collection': os.getenv('MONGODB_COLLECTION', 'applications'),
        'queries_file': os.getenv('QUERIES_FILE', 'data/mongodb_queries.json'),
        'output_file': 'data/Query_Execution_output.csv',
        'timeout': int(os.getenv('TIMEOUT_SECONDS', '60'))
    }


def save_results_to_csv(config, results):
    output_file = config['output_file']
    epoch = int(datetime.now().timestamp())
    column_header = f"{config['collection']}_{epoch}"
    
    # Read existing CSV if it exists
    existing_data = {}
    fieldnames = ['Query Description']
    
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames.copy() if reader.fieldnames else ['Query Description']
            for row in reader:
                existing_data[row['Query Description']] = row
    
    # Add new column header
    fieldnames.append(column_header)
    
    # Update data with new results
    for result in results:
        desc = result['description']
        if desc not in existing_data:
            existing_data[desc] = {'Query Description': desc}
        existing_data[desc][column_header] = result['time'] if result['status'] == 'SUCCESS' else 'ERROR'
    
    # Write updated CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for desc in existing_data:
            writer.writerow(existing_data[desc])
    
    print(f"💾 Results saved to: {output_file}")
    print(f"📊 Column: {column_header}")


def execute_queries(config):
    print(f"🔧 Reading queries from {config['queries_file']}...")
    
    with open(config['queries_file'], 'r') as f:
        queries = json.load(f)
    
    print(f"📋 Found {len(queries)} queries to execute\n")
    
    results = []
    
    for q in queries:
        description = q['description']
        query = q['query']
        
        # Replace collection placeholder with actual collection from config
        query = query.replace('{{collection}}', config['collection'])
        
        print(f"🔍 {description}")
        
        try:
            js_script = f"""
            var targetDb = db.getSiblingDB('{config['database']}');
            var startMs = Date.now();
            var result = {query};
            var endMs = Date.now();
            print('EXEC_TIME:' + (endMs - startMs));
            """
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                f.write(js_script)
                script_file = f.name
            
            result = subprocess.run(
                ['mongosh', config['connection_string'], '--quiet', '--file', script_file],
                capture_output=True, text=True, timeout=config['timeout']
            )
            
            if result.returncode == 0:
                db_time = 0
                for line in result.stdout.split('\n'):
                    if line.startswith('EXEC_TIME:'):
                        db_time = int(line.split(':')[1]) / 1000.0
                        break
                
                print(f"   ✅ {db_time:.3f}s")
                status = "SUCCESS"
            else:
                print(f"   ❌ Failed")
                status = "ERROR"
                db_time = 0
            
            results.append({
                'description': description,
                'status': status,
                'time': round(db_time, 3)
            })
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results.append({
                'description': description,
                'status': "ERROR",
                'time': 0
            })
        finally:
            try:
                os.unlink(script_file)
            except:
                pass
    
    successful = len([r for r in results if r['status'] == 'SUCCESS'])
    total_time = sum(r['time'] for r in results)
    avg_time = total_time / len(results) if results else 0
    
    print(f"\n📊 Summary: {successful}/{len(results)} queries executed")
    print(f"⏱️ Total: {total_time:.3f}s | Avg: {avg_time:.3f}s\n")
    
    save_results_to_csv(config, results)
    
    return True


def main():
    print("=" * 60)
    print("MongoDB Query Executor")
    print("=" * 60 + "\n")
    
    config = load_config()
    print(f"Database: {config['database']}")
    print(f"Collection: {config['collection']}\n")
    
    if not os.path.exists(config['queries_file']):
        print(f"❌ {config['queries_file']} not found")
        return 1
    
    execute_queries(config)
    
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
