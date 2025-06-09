#!/usr/bin/env python3

import asyncio
import sys
import time
import signal
from kvstore.distributed_node import DistributedNode

class KVStoreCluster:
    def __init__(self):
        self.nodes = []
        self.running = False
    
    async def start_cluster(self, num_nodes=3, replication_factor=3):
        print("Starting Distributed KV Store Cluster...")
        print(f"   Nodes: {num_nodes}")
        print(f"   Replication Factor: {replication_factor}")
        print("=" * 50)
        
        for i in range(num_nodes):
            node_id = f"node{i+1}"
            port = 8001 + i
            node = DistributedNode(node_id, port=port, replication_factor=replication_factor)
            self.nodes.append(node)
        
        for node in self.nodes:
            await node.start()
            await asyncio.sleep(0.5)
        
        print("\nJoining nodes to cluster...")
        
        for i in range(1, len(self.nodes)):
            await self.nodes[i].join_cluster("http://localhost:8001")
            await asyncio.sleep(0.5)
        
        print("\nCluster is ready!")
        print(f"   Node addresses: {[node.address for node in self.nodes]}")
        print("   All nodes are connected and synchronized")
        
        self.running = True
        return self.nodes
    
    async def stop_cluster(self):
        """Stop all nodes"""
        print("\nStopping cluster...")
        for node in self.nodes:
            await node.stop()
        self.running = False
        print("Cluster stopped")
    
    async def keep_running(self):
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass

async def run_cluster_only():
    cluster = KVStoreCluster()
    
    try:
        await cluster.start_cluster()
        
        print("\n" + "=" * 60)
        print("CLUSTER READY FOR CONNECTIONS!")
        print("=" * 60)
        print("You can now:")
        print("• Run the shell: python shell.py")
        print("• Use curl commands")
        print("• Connect other applications")
        print("\nPress Ctrl+C to stop the cluster")
        print("=" * 60)
        
        await cluster.keep_running()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await cluster.stop_cluster()

async def run_cluster_and_shell():
    from shell import KVStoreShell
    
    cluster = KVStoreCluster()
    
    try:
        await cluster.start_cluster()
        
        print("\n" + "=" * 60)
        print("CLUSTER READY! Starting interactive shell...")
        print("=" * 60)
        
        await asyncio.sleep(2)
        
        shell = KVStoreShell()
        await shell.start()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await cluster.stop_cluster()

def show_menu():
    print("Distributed Key-Value Store")
    print("=" * 40)
    print("Choose an option:")
    print("1. Start cluster + interactive shell")
    print("2. Start cluster only (background)")
    print("3. Help")
    print("4. Exit")
    print("=" * 40)

async def run_help():
    print("\nHelp - Distributed KV Store")
    print("=" * 50)
    print("OPTION 1: Cluster + Shell")
    print("  • Starts 3-node cluster automatically")
    print("  • Opens interactive shell")
    print("  • Use commands like: put user1 Alice")
    print("  • Type 'help' in shell for more commands")
    print()
    print("OPTION 2: Cluster Only")
    print("  • Starts 3-node cluster in background")
    print("  • Use with external tools:")
    print("    - python shell.py (in another terminal)")
    print("    - curl commands")
    print("    - Your own applications")
    print()
    print("CLUSTER DETAILS:")
    print("  • Node 1: http://localhost:8001")
    print("  • Node 2: http://localhost:8002") 
    print("  • Node 3: http://localhost:8003")
    print("  • Replication factor: 3")
    print("  • Consistency levels: one, quorum, all")
    print()
    print("EXAMPLE USAGE:")
    print("  # Start everything")
    print("  python main.py")
    print("  > 1")
    print("  kvstore[1]> put user1 Alice all")
    print("  kvstore[1]> get user1")
    print("  kvstore[1]> status")
    print()
    print("FEATURES:")
    print("  - Consistent hashing")
    print("  - Automatic replication") 
    print("  - Fault tolerance")
    print("  - Configurable consistency")
    print("  - Interactive shell")
    print("  - REST API")

async def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == "--cluster":
            await run_cluster_only()
            return
        elif sys.argv[1] == "--shell":
            await run_cluster_and_shell()
            return
        elif sys.argv[1] == "--help":
            await run_help()
            return
    
    while True:
        try:
            show_menu()
            choice = input("Enter choice (1-4): ").strip()
            
            if choice == "1":
                await run_cluster_and_shell()
                break
            elif choice == "2":
                await run_cluster_only()
                break
            elif choice == "3":
                await run_help()
                input("\nPress Enter to continue...")
                print("\n" + "=" * 50 + "\n")
            elif choice == "4":
                print("Goodbye!")
                break
            else:
                print("Invalid choice. Please try again.\n")
                
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except EOFError:
            break

if __name__ == "__main__":
    if sys.platform != "win32":
        # Unix-like systems
        def signal_handler(sig, frame):
            print("\nReceived interrupt signal, shutting down...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)