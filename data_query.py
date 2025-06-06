from flask import Flask, jsonify, request, send_file
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

app = Flask(__name__)

# File paths
DATA_DIR = "project/data"
JSON_PATH = os.path.join(DATA_DIR, "random_data.json")
BIN_PATH = os.path.join(DATA_DIR, "random_data.bin")

# Load binary data
def load_bin():
    with open(BIN_PATH, "rb") as f:
        return pickle.load(f)

# Load JSON 
def load_json():
    with open(JSON_PATH, "r") as f:
        return json.load(f)

# Wrap profiler 
def profiler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            result = func(*args, **kwargs)
        finally:
            profiler.disable()
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats("cumtime")
            ps.print_stats()
            print(s.getvalue())
        return result
    return wrapper 

# DB connection
def insert_users_to_db(users):
    con = psycopg2.connect(
        user='postgres',
        password='123',
        host='localhost',
        port='5432',
        database='postgres'
    )

    cursor = con.cursor()

    # Recreate table
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
    # Make indexes for fast search
    cursor.execute("""
        CREATE INDEX idx_main_email ON main(email);
        CREATE INDEX idx_main_employed ON main(employed);
        CREATE INDEX idx_main_age ON main(age);
    """)

    # Put users into table
    query = """
        INSERT INTO main (person_name, person_surname, age, email, employed, upload_timestamp) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (email) DO NOTHING;
    """

    for user in users:
        name_parts = user["name"].split()
        name = name_parts[0]
        surname = name_parts[1] if len(name_parts) > 1 else ""

        cursor.execute(query, (
            name,
            surname,
            str(user["age"]),
            user["email"],
            user["employed"],
            datetime.utcnow()
        ))

    con.commit()
    cursor.close()
    con.close()

# Filter users by query params
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

# List users with params
@app.route("/users", methods=["GET"])
@profiler
def get_users():
    users = load_json()
    filtered = filter_users(users, request.args)

    return jsonify(filtered)

# Upload to postgresql and minio with params 
@app.route("/upload_db")
@profiler
def upload_to_db():

    users = load_json()

    filtered = filter_users(users, request.args)  
    
    insert_users_to_db(filtered)

    filtered_path = os.path.join(DATA_DIR, "filtered_users.json")
    with open(filtered_path, "w") as f:
        json.dump(filtered, f, indent=2)

    # Connect to minio
    client = Minio(
        "localhost:9000",  
        access_key='oNrjTPNAUhUGNSjOX2aA',
        secret_key='rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID',
        secure=False
    )

    bucket_name = "uploads"
    object_name = f"filtered_users_{datetime.utcnow().isoformat()}.json"

   
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Put users json list into bucket
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=filtered_path,
        content_type="application/json"
    )

    return jsonify({
        "status": "success",
        "inserted_to_db": len(filtered),
        "minio_path": f"{bucket_name}/{object_name}",
        "users": filtered
    })

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    app.run(debug=True)
