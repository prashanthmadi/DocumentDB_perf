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


def get_explain(config, query, explain_mode):
    """Execute explain query and return result."""
    explain_query = query
    
    # Handle different query types
    # Order matters: check for specific patterns first, then general ones
    
    if '.toArray()' in explain_query:
        # find().toArray() -> find().explain()
        explain_query = explain_query.replace('.toArray()', f'.explain("{explain_mode}")')
    
    elif 'countDocuments(' in explain_query:
        # countDocuments({filter}) -> explain().countDocuments({filter})
        explain_query = explain_query.replace('countDocuments(', f'explain("{explain_mode}").countDocuments(')
    
    elif 'estimatedDocumentCount()' in explain_query:
        # estimatedDocumentCount() -> explain().estimatedDocumentCount()
        explain_query = explain_query.replace('estimatedDocumentCount()', f'explain("{explain_mode}").estimatedDocumentCount()')
    
    elif '.count()' in explain_query:
        # Deprecated count() -> explain().count()
        explain_query = explain_query.replace('.count()', f'.explain("{explain_mode}").count()')
    
    elif '.aggregate(' in explain_query:
        # aggregate([...]) -> aggregate([...]).explain()
        # Find the closing parenthesis for aggregate
        if explain_query.rstrip().endswith(')'):
            explain_query = explain_query.rstrip() + f'.explain("{explain_mode}")'
        else:
            # Has trailing methods - insert before first method after aggregate
            explain_query = explain_query + f'.explain("{explain_mode}")'
    
    elif '.find(' in explain_query:
        # find({filter}) or find({filter}).sort() etc.
        # Add explain at the end
        if explain_query.rstrip().endswith(')'):
            explain_query = explain_query.rstrip() + f'.explain("{explain_mode}")'
        else:
            explain_query = explain_query + f'.explain("{explain_mode}")'
    
    else:
        # Default: append explain at the end
        explain_query = explain_query + f'.explain("{explain_mode}")'
    
    js_script = f"""
    var targetDb = db.getSiblingDB('{config['database']}');
    var explainResult = {explain_query};
    print(JSON.stringify(explainResult, null, 2));
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
        f.write(js_script)
        script_file = f.name
    
    try:
        result = subprocess.run(
            ['mongosh', config['connection_string'], '--quiet', '--file', script_file],
            capture_output=True, text=True, timeout=config['timeout']
        )
        os.unlink(script_file)
        return result
    except subprocess.TimeoutExpired:
        try:
            os.unlink(script_file)
        except:
            pass
        raise  # Re-raise TimeoutExpired so it can be caught above
    except Exception:
        try:
            os.unlink(script_file)
        except:
            pass
        raise


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
            
            # Try allPlansExecution first
            try:
                result = get_explain(config, query, 'allPlansExecution')
                
                if result.returncode == 0:
                    out_file.write(f"Explain Output (mode: allPlansExecution):\n")
                    out_file.write(result.stdout)
                    out_file.write("\n")
                    print(f"   ✅ Captured (allPlansExecution)")
                    continue  # Skip to next query
                    
                # Check if it's a timeout error
                if 'command timeout' in result.stderr or 'timed out' in result.stderr.lower():
                    raise subprocess.TimeoutExpired(cmd=None, timeout=config['timeout'])
                    
                # Other errors
                out_file.write("ERROR:\n")
                out_file.write(result.stderr)
                out_file.write("\n")
                print(f"   ❌ Failed")
                    
            except subprocess.TimeoutExpired:
                # Timeout (server or client) - fallback to executionStats
                out_file.write(f"Note: allPlansExecution timed out, using executionStats instead...\n\n")
                print(f"   ⏱️ Timeout on allPlansExecution, falling back to executionStats...")
                
                try:
                    result = get_explain(config, query, 'executionStats')
                    if result.returncode == 0:
                        out_file.write(f"Explain Output (mode: executionStats):\n")
                        out_file.write(result.stdout)
                        out_file.write("\n")
                        print(f"   ✅ Captured (executionStats)")
                    else:
                        out_file.write("ERROR:\n")
                        out_file.write(result.stderr)
                        out_file.write("\n")
                        print(f"   ❌ Failed")
                except Exception as e:
                    out_file.write(f"ERROR: {e}\n")
                    print(f"   ❌ Error: {e}")
                    
            except Exception as e:
                out_file.write(f"ERROR: {e}\n")
                print(f"   ❌ Error: {e}")
            
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
