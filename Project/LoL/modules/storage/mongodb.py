import os
import re
from pymongo import MongoClient


# Base Functions
def get_client():
    return MongoClient(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 27017)),
        username=os.getenv("DB_USER", os.getenv("DB_USER", "user")),
        password=os.getenv("DB_PSWD", os.getenv("DB_PSWD", "password")),
    )


def get_database(client, db_name):
    return client[db_name]


def get_collection(db, collection_name):
    return db[collection_name]


def insert(collection, data):
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)


def find(collection, query):
    return collection.find(query)


def update(collection, query, update_data):
    collection.update_many(query, {"$set": update_data}, upsert=True)


def delete(collection, query):
    collection.delete_many(query)


if __name__ == "__main__":
    client = get_client()
    db = get_database(client, "test_db")
    collection = get_collection(db, "test_collection")

    # Example usage
    insert(collection, {"name": "John", "age": 30})
    print(list(find(collection, {"name": "John"})))
    update(collection, {"name": "John"}, {"age": 31})
    print(list(find(collection, {"name": "John"})))
    delete(collection, {"name": "John"})
    print(list(find(collection, {"name": "John"})))
