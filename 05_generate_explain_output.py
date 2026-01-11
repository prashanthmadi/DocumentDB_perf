"""
Generate MongoDB explain output for queries.

Usage: python 05_generate_explain_output.py
"""

import subprocess
import sys
import json
import os
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
        'timeout': int(os.getenv('EXPLAIN_TIMEOUT_SECONDS', '300'))
    }


def generate_explain_output(config):
    print(f"🔧 Reading queries from {config['queries_file']}...")
    
    with open(config['queries_file'], 'r') as f:
        queries = json.load(f)
    
    print(f"📋 Found {len(queries)} queries to explain\n")
    
    epoch = int(datetime.now().timestamp())
    output_file = f"data/explain_out_{epoch}.txt"
    
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write(f"MongoDB Explain Output\n")
        out_file.write(f"Generated: {datetime.now().isoformat()}\n")
        out_file.write(f"Database: {config['database']}\n")
        out_file.write(f"Collection: {config['collection']}\n")
        out_file.write("=" * 80 + "\n\n")
        
        for idx, q in enumerate(queries, 1):
            description = q['description']
            query = q['query']
            
            # Replace collection placeholder with actual collection from config
            query = query.replace('{{collection}}', config['collection'])
            
            print(f"🔍 [{idx}/{len(queries)}] {description}")
            
            out_file.write(f"Query {idx}: {description}\n")
            out_file.write("-" * 80 + "\n")
            out_file.write(f"Original Query: {query}\n\n")
            
            try:
                # Wrap query with explain("allPlansExecution")
                explain_query = query
                
                # Handle different query types
                if '.toArray()' in explain_query:
                    explain_query = explain_query.replace('.toArray()', '.explain("allPlansExecution")')
                elif '.count()' in explain_query:
                    explain_query = explain_query.replace('.count()', '.explain("allPlansExecution")')
                elif 'countDocuments(' in explain_query:
                    # For countDocuments, we need to use find().explain()
                    explain_query = explain_query.replace('countDocuments(', 'find(')
                    explain_query = explain_query + '.explain("allPlansExecution")'
                else:
                    # Default: add explain
                    explain_query = explain_query + '.explain("allPlansExecution")'
                
                js_script = f"""
                var targetDb = db.getSiblingDB('{config['database']}');
                var explainResult = {explain_query};
                print(JSON.stringify(explainResult, null, 2));
                """
                
                with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                    f.write(js_script)
                    script_file = f.name
                
                result = subprocess.run(
                    ['mongosh', config['connection_string'], '--quiet', '--file', script_file],
                    capture_output=True, text=True, timeout=config['timeout']
                )
                
                if result.returncode == 0:
                    out_file.write("Explain Output:\n")
                    out_file.write(result.stdout)
                    out_file.write("\n")
                    print(f"   ✅ Captured")
                else:
                    out_file.write("ERROR:\n")
                    out_file.write(result.stderr)
                    out_file.write("\n")
                    print(f"   ❌ Failed")
                
            except Exception as e:
                out_file.write(f"ERROR: {e}\n")
                print(f"   ❌ Error: {e}")
            finally:
                try:
                    os.unlink(script_file)
                except:
                    pass
            
            out_file.write("\n" + "=" * 80 + "\n\n")
    
    print(f"\n💾 Explain output saved to: {output_file}")
    return True


def main():
    print("=" * 60)
    print("MongoDB Explain Generator")
    print("=" * 60 + "\n")
    
    config = load_config()
    print(f"Database: {config['database']}")
    print(f"Collection: {config['collection']}\n")
    
    if not os.path.exists(config['queries_file']):
        print(f"❌ {config['queries_file']} not found")
        return 1
    
    generate_explain_output(config)
    
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
