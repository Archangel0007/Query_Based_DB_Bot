import boto3
from pymongo import MongoClient
import argparse
import os
import uuid
import decimal


def _convert_decimals(obj):
    """Recursively convert Decimal objects to int or float for JSON serialization."""
    if isinstance(obj, list):
        return [_convert_decimals(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, decimal.Decimal):
        # If it's a whole number, return int, else float
        try:
            if obj % 1 == 0:
                return int(obj)
            return float(obj)
        except Exception:
            return float(obj)
    return obj

def fetch_from_dynamodb(access_key, secret_key, region, table_name):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # Use pagination to retrieve the entire table (scan can be limited to 1MB per call)
    items = []
    try:
        response = table.scan()
        items.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
    except Exception as e:
        # Re-raise so callers can handle HTTP responses
        raise

    # Convert Decimal instances to native Python numbers for JSON serialization
    return _convert_decimals(items)

def fetch_from_cosmosdb(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find())

def fetch_from_s3(access_key, secret_key, region, bucket_name, object_key, local_filename):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3 = session.client('s3')
    s3.download_file(bucket_name, object_key, local_filename)
    return local_filename


def _write_json_safe(obj, path):
    import json
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    except TypeError:
        # Fallback: coerce non-serializable items to str
        def _coerce(o):
            try:
                return json.loads(json.dumps(o))
            except Exception:
                return str(o)
        safe = _coerce(obj)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(safe, f, indent=2, ensure_ascii=False)


def main():
    """Simple CLI for testing fetch helpers. Writes results to Run_Space/test_fetch/"""
    

    parser = argparse.ArgumentParser(description="Test data fetch helpers (DynamoDB, S3, CosmosDB)")
    sub = parser.add_subparsers(dest='cmd')

    p_d = sub.add_parser('dynamodb', help='Fetch from DynamoDB')
    p_d.add_argument('--table', required=True)
    p_d.add_argument('--region', required=False)
    p_d.add_argument('--access-key', required=False)
    p_d.add_argument('--secret-key', required=False)

    p_s = sub.add_parser('s3', help='Fetch object from S3')
    p_s.add_argument('--bucket', required=True)
    p_s.add_argument('--key', required=True)
    p_s.add_argument('--region', required=False)
    p_s.add_argument('--access-key', required=False)
    p_s.add_argument('--secret-key', required=False)

    p_c = sub.add_parser('cosmos', help='Fetch from Cosmos/Mongo')
    p_c.add_argument('--uri', required=True)
    p_c.add_argument('--db', required=True)
    p_c.add_argument('--collection', required=True)

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return

    base = os.path.join(os.getcwd(), '../Run_Space', 'test_fetch_' + uuid.uuid4().hex[:8])
    os.makedirs(base, exist_ok=True)

    try:
        if args.cmd == 'dynamodb':
            print(f"[TEST] Fetching DynamoDB table {args.table} (region={args.region})")
            items = fetch_from_dynamodb(args.access_key, args.secret_key, args.region, args.table)
            out = os.path.join(base, f"{args.table}.json")
            _write_json_safe(items, out)
            print(f"Wrote {len(items) if items is not None else 0} items to {out}")

        elif args.cmd == 's3':
            local = os.path.join(base, os.path.basename(args.key) or 's3_object')
            print(f"[TEST] Downloading s3://{args.bucket}/{args.key} -> {local}")
            fetch_from_s3(args.access_key, args.secret_key, args.region, args.bucket, args.key, local)
            print(f"Downloaded to {local}")

        elif args.cmd == 'cosmos':
            print(f"[TEST] Fetching CosmosDB {args.db}/{args.collection}")
            docs = fetch_from_cosmosdb(args.uri, args.db, args.collection)
            out = os.path.join(base, f"{args.db}__{args.collection}.json")
            _write_json_safe(docs, out)
            print(f"Wrote {len(docs) if docs is not None else 0} documents to {out}")

    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")
        print("Check credentials, network access, and that the resource identifiers are correct.")


if __name__ == '__main__':
    main()
