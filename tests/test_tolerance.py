import asyncio
from kvstore.distributed_node import DistributedNode

async def test_two_nodes():
    print("=== Testing Two Node Cluster (Simulating Node1 Failure) ===")
    
    node2 = DistributedNode("node2", port=8002, replication_factor=3) 
    node3 = DistributedNode("node3", port=8003, replication_factor=3)
    
    await node2.start()
    await node3.start()
    
    await asyncio.sleep(1)
    
    await node3.join_cluster("http://localhost:8002")
    
    print("\nTwo-node cluster ready!")
    print("Test commands:")
    print("curl -X PUT http://localhost:8002/keys/test -H 'Content-Type: application/json' -d '{\"value\": \"survivor_data\"}'")
    print("curl http://localhost:8003/keys/test")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        await node2.stop()
        await node3.stop()

if __name__ == "__main__":
    asyncio.run(test_two_nodes())