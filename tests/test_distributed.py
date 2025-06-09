import asyncio
from kvstore.distributed_node import DistributedNode

async def test_replicated_cluster():
    print("=== Testing Replicated Distributed Cluster ===")
    
    node1 = DistributedNode("node1", port=8001, replication_factor=3)
    node2 = DistributedNode("node2", port=8002, replication_factor=3) 
    node3 = DistributedNode("node3", port=8003, replication_factor=3)
    
    await node1.start()
    await node2.start()
    await node3.start()
    
    await asyncio.sleep(1)
    
    await node2.join_cluster("http://localhost:8001")
    await node3.join_cluster("http://localhost:8001")
    
    print("\nReplicated cluster is ready!")
    print("Now every key will be stored on ALL 3 nodes!")
    print("\nTry these commands:")
    print("curl -X PUT http://localhost:8001/keys/user1 -H 'Content-Type: application/json' -d '{\"value\": \"Alice\"}'")
    print("curl http://localhost:8002/keys/user1  # Should work from any node!")
    print("curl http://localhost:8003/admin/status")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        await node1.stop()
        await node2.stop()
        await node3.stop()

if __name__ == "__main__":
    asyncio.run(test_replicated_cluster())