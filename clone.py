import json
import os
from pymongo import MongoClient
from os import environ

# Database configuration
DATABASE_URI = environ.get('DATABASE_URI', "mongodb+srv://filtermovie:M3hmXVTRv5rJd30k@cluster0.fnnkaqm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DATABASE_NAME = environ.get('DATABASE_NAME', "Cluster0")
COLLECTION_NAME = environ.get('COLLECTION_NAME', 'Telegram_files')

# Output JSON files
OUTPUT_PREFIX = "db"
OUTPUT_SIZE_LIMIT = 1 * 1024 * 1024  # 1 MB

def save_in_chunks(documents, prefix, size_limit):
    chunk = []
    current_size = 0
    file_index = 1

    for doc in documents:
        # Convert document to JSON string to measure size
        doc_json = json.dumps(doc, ensure_ascii=False)
        doc_size = len(doc_json.encode('utf-8'))

        # Check if adding this document exceeds the size limit
        if current_size + doc_size > size_limit:
            # Save current chunk to file
            output_file = f"{prefix}_{file_index}.json"
            with open(output_file, "w", encoding="utf-8") as file:
                json.dump(chunk, file, indent=4, ensure_ascii=False)

            print(f"Saved {len(chunk)} documents to {output_file}")
            
            # Start a new chunk
            chunk = []
            current_size = 0
            file_index += 1

        # Add document to the current chunk
        chunk.append(doc)
        current_size += doc_size

    # Save the last chunk
    if chunk:
        output_file = f"{prefix}_{file_index}.json"
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(chunk, file, indent=4, ensure_ascii=False)

        print(f"Saved {len(chunk)} documents to {output_file}")

def clone_and_split_database():
    try:
        # Connect to MongoDB
        client = MongoClient(DATABASE_URI)
        database = client[DATABASE_NAME]
        collection = database[COLLECTION_NAME]

        # Fetch all documents
        documents = list(collection.find())

        # Convert ObjectId to string for JSON serialization
        for doc in documents:
            doc["_id"] = str(doc["_id"])

        # Save documents in chunks of specified size
        save_in_chunks(documents, OUTPUT_PREFIX, OUTPUT_SIZE_LIMIT)

        print("Database cloned and split successfully.")

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    clone_and_split_database()
