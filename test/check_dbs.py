#!/usr/bin/env python3
import chromadb
from chromadb.config import Settings

print('=== chroma_db (mock) ===')
client1 = chromadb.PersistentClient(path='./chroma_db', settings=Settings(anonymized_telemetry=False))
colls1 = client1.list_collections()
for c in colls1:
    try:
        count = client1.get_collection(c.name).count()
        print(f'Collection: {c.name} - Documents: {count}')
    except Exception as e:
        print(f'Collection: {c.name} - Error: {e}')

print()
print('=== chroma_db_openai ===')        
client2 = chromadb.PersistentClient(path='./chroma_db_openai', settings=Settings(anonymized_telemetry=False))
colls2 = client2.list_collections()
for c in colls2:
    try:
        count = client2.get_collection(c.name).count()
        print(f'Collection: {c.name} - Documents: {count}')
    except Exception as e:
        print(f'Collection: {c.name} - Error: {e}')
