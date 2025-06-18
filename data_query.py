from flask import Flask, jsonify, request
import pickle
import json
import os
import cProfile
import pstats
import io
import psycopg2
from datetime import datetime
from minio import Minio
import functools
import time
import threading
from psycopg2.extras import execute_values

app = Flask(__name__)

# Paths to store data locally
DATA_DIR = "project/data"
JSON_PATH = os.path.join(DATA_DIR, "random_data.json")
BIN_PATH = os.path.join(DATA_DIR, "random_data.bin")

# Prevent concurrent profiling
_profiler_lock = threading.Lock()

# Profiling decorator
def profiler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not _profiler_lock.acquire(blocking=False):
            return func(*args, **kwargs)

        profiler_instance = cProfile.Profile()
        profiler_instance.enable()
        try:
            result = func(*args, **kwargs)
        finally:
            profiler_instance.disable()
            s = io.StringIO()
            ps = pstats.Stats(profiler_instance, stream=s).sort_stats("cumtime")
            ps.print_stats()
            print(s.getvalue())
            _profiler_lock.release()
        return result
    return wrapper

# Load users from JSON
def load_json():
    with open(JSON_PATH, "r") as f:
        return json.load(f)

# Filter users based on query parameters
def filter_users(users, args):
    min_age = args.get("min_age", type=int)
    max_age = args.get("max_age", type=int)
    city = args.get("city", type=str)
    active = args.get("employed", type=str)

    if min_age is not None:
        users = [u for u in users if u["age"] >= min_age]
    if max_age is not None:
        users = [u for u in users if u["age"] <= max_age]
    if city:
        users = [u for u in users if u["address"]["city"].lower() == city.lower()]
    if active in ["true", "false"]:
        is_active = active == "true"
        users = [u for u in users if u["employed"] == is_active]

    return users

# Insert filtered users into PostgreSQL table
def insert_users_to_db(users):
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()

    # Recreate table and indexes
    cursor.execute("DROP TABLE IF EXISTS main;")
    cursor.execute("""
        CREATE TABLE main (
            id SERIAL PRIMARY KEY,
            person_name VARCHAR(255),
            person_surname VARCHAR(255),
            age INTEGER,
            email VARCHAR(255) UNIQUE,
            employed BOOL,
            upload_timestamp timestamptz
        );
    """)
    cursor.execute("CREATE INDEX idx_main_email ON main(email);")
    cursor.execute("CREATE INDEX idx_main_employed ON main(employed);")
    cursor.execute("CREATE INDEX idx_main_age ON main(age);")

    query = """
        INSERT INTO main (person_name, person_surname, age, email, employed, upload_timestamp) 
        VALUES %s
        ON CONFLICT (email) DO NOTHING;
    """

    values = []
    for user in users:
        name_parts = user["name"].split()
        name = name_parts[0]
        surname = name_parts[1] if len(name_parts) > 1 else ""
        values.append((
            name,
            surname,
            user["age"],
            user["email"],
            user["employed"],
            datetime.utcnow()
        ))

    execute_values(cursor, query, values)
    con.commit()
    cursor.close()
    con.close()

# Create performance metrics table 
def create_performance_table():
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id SERIAL PRIMARY KEY,
            operation_type VARCHAR(50),
            duration_seconds FLOAT,
            records_processed INTEGER,
            timestamp TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    con.commit()
    cursor.close()
    con.close()

# Create table for binary data
def create_binary_data_table():
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS binary_data (
            id SERIAL PRIMARY KEY,
            data BYTEA,
            description TEXT,
            upload_timestamp timestamptz DEFAULT NOW()
        );
    """)
    con.commit()
    cursor.close()
    con.close()

# Store performance stats
def insert_performance_metrics(operation_type, duration, records_count):
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()
    cursor.execute("""
        INSERT INTO performance_metrics (operation_type, duration_seconds, records_processed)
        VALUES (%s, %s, %s);
    """, (operation_type, duration, records_count))
    con.commit()
    cursor.close()
    con.close()

# Store binary data into database
def insert_binary_data(bin_data, description=""):
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()
    cursor.execute("""
        INSERT INTO binary_data (data, description)
        VALUES (%s, %s);
    """, (psycopg2.Binary(bin_data), description))
    con.commit()
    cursor.close()
    con.close()

# GET /users - filter and return users
@app.route("/users", methods=["GET"])
@profiler
def get_users():
    users = load_json()
    filtered = filter_users(users, request.args)
    return jsonify(filtered)

# GET /upload_db - upload to DB and MinIO
@app.route("/upload_db")
@profiler
def upload_to_db():
    users = load_json()
    filtered = filter_users(users, request.args)

    # Insert to PostgreSQL
    start_pg = time.time()
    insert_users_to_db(filtered)
    dur_pg = time.time() - start_pg
    insert_performance_metrics("postgres_upload", dur_pg, len(filtered))

    # Save filtered data locally
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filtered_path = os.path.join(DATA_DIR, f"filtered_users_{ts}.json")
    with open(filtered_path, "w") as f:
        json.dump(filtered, f, indent=2)

    # Upload to MinIO
    start_minio = time.time()
    client = Minio(
        "localhost:9000",
        access_key='oNrjTPNAUhUGNSjOX2aA',
        secret_key='rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID',
        secure=False
    )
    bucket = "uploads"
    object_name = os.path.basename(filtered_path)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.fput_object(
        bucket_name=bucket,
        object_name=object_name,
        file_path=filtered_path,
        content_type="application/json"
    )
    dur_minio = time.time() - start_minio
    insert_performance_metrics("minio_upload", dur_minio, len(filtered))

    return jsonify({
        "status": "success",
        "inserted_to_db": len(filtered),
        "minio_path": f"{bucket}/{object_name}",
        "timings": {
            "postgres_upload_seconds": dur_pg,
            "minio_upload_seconds": dur_minio,
            "total_seconds": dur_pg + dur_minio
        },
        "users": filtered
    })

# GET /upload_bin - save sorted binary data
@app.route("/upload_bin")
@profiler
def upload_binary_data():
    users = load_json()
    filtered = filter_users(users, request.args)
    sorted_users = sorted(filtered, key=lambda u: u["age"])
    bin_data = pickle.dumps(sorted_users)

    insert_binary_data(bin_data, description="Sorted users by age")

    # Save binary to file and upload to MinIO
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    bin_filename = f"sorted_users_{ts}.bin"
    bin_filepath = os.path.join(DATA_DIR, bin_filename)
    with open(bin_filepath, "wb") as f:
        f.write(bin_data)

    client = Minio(
        "localhost:9000",
        access_key='oNrjTPNAUhUGNSjOX2aA',
        secret_key='rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID',
        secure=False
    )
    bucket = "binary-uploads"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    client.fput_object(
        bucket_name=bucket,
        object_name=bin_filename,
        file_path=bin_filepath,
        content_type="application/octet-stream"
    )

    os.remove(bin_filepath)

    return jsonify({
        "status": "success",
        "records_count": len(sorted_users),
        "message": "Binary data uploaded to database and MinIO",
        "minio_path": f"{bucket}/{bin_filename}"
    })

# GET /upload_all - combined upload: DB + binary + MinIO
@app.route("/upload_all")
@profiler
def upload_all():
    users = load_json()
    filtered = filter_users(users, request.args)

    # Upload to PostgreSQL
    start_pg = time.time()
    insert_users_to_db(filtered)
    dur_pg = time.time() - start_pg
    insert_performance_metrics("postgres_upload", dur_pg, len(filtered))

    # Save JSON
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    filtered_json_path = os.path.join(DATA_DIR, f"filtered_users_{ts}.json")
    with open(filtered_json_path, "w") as f:
        json.dump(filtered, f, indent=2)

    # Upload binary to DB
    start_bin_pg = time.time()
    bin_data = pickle.dumps(filtered)
    con = psycopg2.connect(user='postgres', password='123', host='localhost', port='5432', database='postgres')
    cursor = con.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS binary_data (
            id SERIAL PRIMARY KEY,
            data BYTEA,
            description TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    cursor.execute("INSERT INTO binary_data (data, description) VALUES (%s, %s);", (bin_data, f"filtered_users_{ts}"))
    con.commit()
    cursor.close()
    con.close()
    dur_bin_pg = time.time() - start_bin_pg
    insert_performance_metrics("binary_pg_upload", dur_bin_pg, len(filtered))

    # Upload both to MinIO
    binary_file_path = os.path.join(DATA_DIR, f"filtered_users_{ts}.bin")
    with open(binary_file_path, "wb") as f:
        f.write(bin_data)

    client = Minio(
        "localhost:9000",
        access_key='oNrjTPNAUhUGNSjOX2aA',
        secret_key='rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID',
        secure=False
    )

    start_minio_json = time.time()
    json_bucket = "uploads"
    if not client.bucket_exists(json_bucket):
        client.make_bucket(json_bucket)
    client.fput_object(
        bucket_name=json_bucket,
        object_name=os.path.basename(filtered_json_path),
        file_path=filtered_json_path,
        content_type="application/json"
    )
    dur_minio_json = time.time() - start_minio_json
    insert_performance_metrics("minio_json_upload", dur_minio_json, len(filtered))

    start_minio_bin = time.time()
    bin_bucket = "binary-uploads"
    if not client.bucket_exists(bin_bucket):
        client.make_bucket(bin_bucket)
    client.fput_object(
        bucket_name=bin_bucket,
        object_name=os.path.basename(binary_file_path),
        file_path=binary_file_path,
        content_type="application/octet-stream"
    )
    dur_minio_bin = time.time() - start_minio_bin
    insert_performance_metrics("minio_bin_upload", dur_minio_bin, len(filtered))

    return jsonify({
        "status": "success",
        "records": len(filtered),
        "paths": {
            "json_minio": f"{json_bucket}/{os.path.basename(filtered_json_path)}",
            "bin_minio": f"{bin_bucket}/{os.path.basename(binary_file_path)}"
        },
        "timings": {
            "postgres_upload_seconds": dur_pg,
            "binary_pg_upload_seconds": dur_bin_pg,
            "minio_json_upload_seconds": dur_minio_json,
            "minio_bin_upload_seconds": dur_minio_bin,
            "total_seconds": dur_pg + dur_bin_pg + dur_minio_json + dur_minio_bin
        }
    })

# GET /metrics - show stats from database
@app.route("/metrics")
def get_metrics():
    con = psycopg2.connect(
        user='postgres', password='123',
        host='localhost', port='5432', database='postgres'
    )
    cursor = con.cursor()

    cursor.execute("""
        SELECT operation_type, 
               AVG(duration_seconds), MIN(duration_seconds),
               MAX(duration_seconds), COUNT(*)
        FROM performance_metrics
        GROUP BY operation_type;
    """)
    agg_results = cursor.fetchall()

    aggregated = [
        {
            "operation": row[0],
            "avg_time_seconds": float(row[1]),
            "min_time_seconds": float(row[2]),
            "max_time_seconds": float(row[3]),
            "operations_count": row[4]
        } for row in agg_results
    ]

    cursor.execute("""
        SELECT operation_type, duration_seconds, records_processed, timestamp
        FROM performance_metrics
        ORDER BY timestamp DESC
        LIMIT 10;
    """)
    recent_results = cursor.fetchall()

    latest = [
        {
            "operation": row[0],
            "duration_seconds": float(row[1]),
            "records_processed": row[2],
            "timestamp": row[3].isoformat()
        } for row in recent_results
    ]

    cursor.close()
    con.close()

    return jsonify({
        "stat_metrics": aggregated,
        "latest_metrics": latest
    })

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    app.run(debug=True)
