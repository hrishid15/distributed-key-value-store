import asyncio
from kvstore.http_server import HTTPKVStore

async def main():
    kv_server = HTTPKVStore(port=8000)
    await kv_server.start()
    
    print("\nTry these commands in another terminal:")
    print("curl -X PUT http://localhost:8000/keys/name -H 'Content-Type: application/json' -d '{\"value\": \"Alice\"}'")
    print("curl http://localhost:8000/keys/name")
    print("curl http://localhost:8000/stats")
    print("curl -X DELETE http://localhost:8000/keys/name")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")

if __name__ == "__main__":
    asyncio.run(main())