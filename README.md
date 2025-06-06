# Generation Data and Data Query API

This project contains a Flask-based REST API for working with generated user data. It includes:

- A **data generator** that creates random user data in JSON and binary formats.
- A **Flask API** that filters, returns, and uploads this data to:
  - A **PostgreSQL database**
  - A **MinIO object storage** bucket

---

## 📁 Project Structure

```
project/
├── data/
│   ├── random_data.json        # Generated user data (JSON)
│   └── random_data.bin         # Same data in binary format
├── generate_data.py            # Script to generate user data
├── data_query.py                      # Flask app to serve and upload data
└── README.md
```

---

## ⚙️ Requirements

- Python 3.7+
- PostgreSQL
- MinIO (S3-compatible object storage)

Install dependencies:

```bash
pip install flask psycopg2-binary minio
```

---

## 🚀 Usage

### 1. Generate Data

This script will generate random user data and save it in `/project/data/`.

```bash
python generate_data.py
```

### 2. Start Flask API

Run the API server:

```bash
python data_query.py
```

The server runs at: `http://127.0.0.1:5000`

---

## 📡 API Endpoints

### `GET /users`

Returns the user data (from `random_data.json`), optionally filtered.

**Query Parameters:**

| Param     | Type   | Description                |
|-----------|--------|----------------------------|
| `min_age` | int    | Minimum age                |
| `max_age` | int    | Maximum age                |
| `city`    | string | Filter by city             |
| `employed`| bool   | `true` or `false`          |

**Example:**
```
GET /users?min_age=25&employed=true
```

---

### `GET /upload_db`

Filters the user data and:

- Inserts it into a local PostgreSQL database.
- Saves it to a `.json` file.
- Uploads the JSON file to MinIO.

**Query Parameters:** Same as `/users`.

**Example:**
```
GET /upload_db?city=Chicago&employed=true
```

---

## 🛠️ Setup: PostgreSQL

Ensure you have a PostgreSQL server running locally with the following credentials:

- **User:** `postgres`
- **Password:** `123`
- **Database:** `postgres`
- **Port:** `5432`

---

## ☁️ Setup: MinIO

Ensure MinIO is running on your machine at:

- **Host:** `localhost:9000`
- **Access Key:** `oNrjTPNAUhUGNSjOX2aA`
- **Secret Key:** `rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID`

Run it via Docker:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=oNrjTPNAUhUGNSjOX2aA" \
  -e "MINIO_ROOT_PASSWORD=rau6jnonycG7jhZQPghW2nUQmFYTUOvQStS5nQID" \
  quay.io/minio/minio server /data --console-address ":9001"
```

---

## 📈 Profiling

API endpoints are wrapped in a profiler and will print performance stats to the console.

---

## ✅ Example Workflow

```bash
python generate_data.py
python data_query.py
# In browser or Postman:
http://127.0.0.1:5000/users?min_age=30&city=Chicago
http://127.0.0.1:5000/upload_db?employed=true
```

---

