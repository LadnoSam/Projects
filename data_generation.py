import json
import random
import string
import pickle
import os
import random

DATA_DIR = os.path.join("project", "data") 

# List of data to make random users
first_names = ["John", "Emma", "Michael", "Olivia", "James", "Sophia", "Daniel"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis"]
domains = ["gmail.com", "outlook.com", "yahoo.com", "example.com"]
streets = ["Main St", "Highland Ave", "Maple Dr", "Oak St", "2nd St", "Sunset Blvd", "Elm St"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Diego", "Dallas"]

# Function for generation json 
def generate_random_json_data():
    first = random.choice(first_names)
    last = random.choice(last_names)
    full_name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@{random.choice(domains)}"
    
    return {
        "id": random.randint(1, 1000),
        "name": full_name,
        "age": random.randint(18, 65),
        "email": email,
        "employed": random.choice([True, False]),
        "address": {
            "street": f"{random.randint(100, 9999)} {random.choice(streets)}",
            "city": random.choice(cities),
            "zipcode": random.randint(10000, 99999)
        }
    }

# Make json file with users
def write_json_to_file(file_path, data):
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

# Make bin file with users
def write_binary_to_file(file_path, data):
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

# Generation and maeking the data
def generate_and_write_data():
    os.makedirs(DATA_DIR, exist_ok=True)

    json_data = [generate_random_json_data() for _ in range(100)]
    write_json_to_file(os.path.join(DATA_DIR, "random_data.json"), json_data)

    bin_data = {entry["id"]: entry for entry in json_data}
    write_binary_to_file(os.path.join(DATA_DIR, "random_data.bin"), bin_data)

    print("Data written to /project/data/: random_data.json and random_data.bin")

# Check bin file 
def check_binary_content():
    bin_path = os.path.join(DATA_DIR, "random_data.bin")
    with open(bin_path, "rb") as f:
        data = pickle.load(f)
        print("BIN content:")
        for user_id, user in list(data.items())[:5]:
            print(f"ID: {user_id} | Name: {user['name']} | Email: {user['email']}")

if __name__ == "__main__":
    generate_and_write_data()
    check_binary_content()

