#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import sys
from typing import List, Optional

class KVStoreShell:
    def __init__(self, nodes: List[str] = None):
        self.nodes = nodes or ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
        self.current_node = 0
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def start(self):
        """Start the shell session"""
        self.session = aiohttp.ClientSession()
        
        print("Welcome to the Distributed KV Store Shell!")
        print(f"Connected to nodes: {', '.join(self.nodes)}")
        print(f"Current node: {self.nodes[self.current_node]}")
        print("\nCommands:")
        print("  put <key> <value> [consistency]   - Store a key-value pair")
        print("  get <key> [consistency]           - Retrieve a value")
        print("  delete <key> [consistency]        - Delete a key")
        print("  status [node_num]                 - Show node status")
        print("  switch <node_num>                 - Switch to different node")
        print("  nodes                             - List all nodes")
        print("  help                              - Show this help")
        print("  quit                              - Exit the shell")
        print("\nConsistency levels: one, quorum, all (default: quorum)")
        print("=" * 60)
        
        while True:
            try:
                # Get command input
                prompt = f"kvstore[{self.current_node + 1}]> "
                command = input(prompt).strip()
                
                if not command:
                    continue
                    
                if command.lower() in ['quit', 'exit', 'q']:
                    break
                    
                await self.execute_command(command)
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                break
        
        await self.session.close()
    
    async def execute_command(self, command: str):
        """Execute a shell command"""
        parts = command.split()
        if not parts:
            return
            
        cmd = parts[0].lower()
        
        try:
            if cmd == 'put' and len(parts) >= 3:
                key = parts[1]
                value = parts[2]
                consistency = parts[3] if len(parts) > 3 else 'quorum'
                await self.put(key, value, consistency)
                
            elif cmd == 'get' and len(parts) >= 2:
                key = parts[1]
                consistency = parts[2] if len(parts) > 2 else 'one'
                await self.get(key, consistency)
                
            elif cmd == 'delete' and len(parts) >= 2:
                key = parts[1]
                consistency = parts[2] if len(parts) > 2 else 'quorum'
                await self.delete(key, consistency)
                
            elif cmd == 'status':
                node_num = int(parts[1]) - 1 if len(parts) > 1 else self.current_node
                await self.status(node_num)
                
            elif cmd == 'switch' and len(parts) >= 2:
                node_num = int(parts[1]) - 1
                self.switch_node(node_num)
                
            elif cmd == 'nodes':
                self.list_nodes()
                
            elif cmd == 'help':
                await self.show_help()
                
            else:
                print("Invalid command. Type 'help' for available commands.")
                
        except Exception as e:
            print(f"Error: {e}")
    
    async def put(self, key: str, value: str, consistency: str = 'quorum'):
        """Store a key-value pair"""
        url = f"{self.nodes[self.current_node]}/keys/{key}"
        data = {"value": value, "consistency": consistency}
        
        try:
            async with self.session.put(url, json=data) as resp:
                result = await resp.json()
                
                if resp.status == 200:
                    print(f"PUT successful!")
                    print(f"   Key: {result['key']}")
                    print(f"   Value: {result['value']}")
                    print(f"   Consistency: {result['consistency_level']}")
                    print(f"   Replicas: {result['successful_replicas']}/{result['attempted_replicas']} (of {result['total_possible_replicas']} possible)")
                    print(f"   Coordinated by: {result['coordinated_by']}")
                else:
                    print(f"PUT failed: {result.get('error', 'Unknown error')}")
                    
        except Exception as e:
            print(f"Network error: {e}")
    
    async def get(self, key: str, consistency: str = 'one'):
        """Retrieve a value"""
        url = f"{self.nodes[self.current_node]}/keys/{key}"
        params = {"consistency": consistency}
        
        try:
            async with self.session.get(url, params=params) as resp:
                result = await resp.json()
                
                if resp.status == 200:
                    print(f"GET successful!")
                    print(f"   Key: {result['key']}")
                    print(f"   Value: {result['value']}")
                    print(f"   Consistency: {result['consistency_level']}")
                    
                    if 'source_node' in result:
                        print(f"   Source: {result['source_node']}")
                    elif 'source_nodes' in result:
                        print(f"   Sources: {result['source_nodes']}")
                        
                    print(f"   Queried: {result['queried_node']}")
                else:
                    print(f"GET failed: {result.get('error', 'Unknown error')}")
                    
        except Exception as e:
            print(f"Network error: {e}")
    
    async def delete(self, key: str, consistency: str = 'quorum'):
        """Delete a key"""
        url = f"{self.nodes[self.current_node]}/keys/{key}"
        params = {"consistency": consistency}
        
        try:
            async with self.session.delete(url, params=params) as resp:
                result = await resp.json()
                
                if resp.status == 200:
                    print(f"DELETE successful!")
                    print(f"   Message: {result['message']}")
                    print(f"   Consistency: {result['consistency_level']}")
                    print(f"   Replicas: {result['successful_replicas']}/{result['attempted_replicas']} (of {result['total_possible_replicas']} possible)")
                else:
                    print(f"DELETE failed: {result.get('error', 'Unknown error')}")
                    
        except Exception as e:
            print(f"Network error: {e}")
    
    async def status(self, node_num: int = None):
        """Show node status"""
        if node_num is None:
            node_num = self.current_node
            
        if node_num < 0 or node_num >= len(self.nodes):
            print(f"Invalid node number. Use 1-{len(self.nodes)}")
            return
            
        url = f"{self.nodes[node_num]}/admin/status"
        
        try:
            async with self.session.get(url) as resp:
                result = await resp.json()
                
                if resp.status == 200:
                    print(f"Node {node_num + 1} Status:")
                    print(f"   Node ID: {result['node_id']}")
                    print(f"   Address: {result['address']}")
                    print(f"   Local Keys: {result['local_keys']}")
                    print(f"   Cluster Nodes: {result['cluster_nodes']}")
                    print(f"   Hash Ring: {result['hash_ring_nodes']}")
                    print(f"   Replication Factor: {result['replication_factor']}")
                    if result['all_keys_sample']:
                        print(f"   Sample Keys: {result['all_keys_sample']}")
                else:
                    print(f"Status failed: {resp.status}")
                    
        except Exception as e:
            print(f"Network error: {e}")
    
    def switch_node(self, node_num: int):
        """Switch to a different node"""
        if 0 <= node_num < len(self.nodes):
            self.current_node = node_num
            print(f"Switched to node {node_num + 1}: {self.nodes[node_num]}")
        else:
            print(f"Invalid node number. Use 1-{len(self.nodes)}")
    
    def list_nodes(self):
        """List all available nodes"""
        print("Available Nodes:")
        for i, node in enumerate(self.nodes):
            marker = " 👈 current" if i == self.current_node else ""
            print(f"   {i + 1}. {node}{marker}")
    
    async def show_help(self):
        """Show help information"""
        print("\n📚 Command Reference:")
        print("╭─────────────────────────────────────────────────────────────╮")
        print("│ BASIC OPERATIONS                                            │")
        print("├─────────────────────────────────────────────────────────────┤")
        print("│ put user1 Alice           - Store with default consistency │")
        print("│ put user1 Alice one       - Store with eventual consistency│")
        print("│ put user1 Alice quorum    - Store with quorum consistency  │")
        print("│ put user1 Alice all       - Store with strong consistency  │")
        print("├─────────────────────────────────────────────────────────────┤")
        print("│ get user1                 - Retrieve with eventual read    │")
        print("│ get user1 quorum          - Retrieve with quorum read      │")
        print("│ get user1 all             - Retrieve with strong read      │")
        print("├─────────────────────────────────────────────────────────────┤")
        print("│ delete user1              - Delete with quorum consistency │")
        print("│ delete user1 all          - Delete with strong consistency │")
        print("├─────────────────────────────────────────────────────────────┤")
        print("│ CLUSTER MANAGEMENT                                          │")
        print("├─────────────────────────────────────────────────────────────┤")
        print("│ status                    - Show current node status       │")
        print("│ status 2                  - Show node 2 status             │")
        print("│ switch 3                  - Switch to node 3               │")
        print("│ nodes                     - List all nodes                 │")
        print("╰─────────────────────────────────────────────────────────────╯")
        print("\nTips:")
        print("   • Use 'one' for fastest writes (least durable)")
        print("   • Use 'quorum' for balanced performance/safety")
        print("   • Use 'all' for strongest consistency (slowest)")
        print("   • Try storing with different consistency levels!")

async def main():
    """Main entry point"""
    # Check if custom nodes provided via command line
    nodes = []
    if len(sys.argv) > 1:
        nodes = sys.argv[1:]
    
    shell = KVStoreShell(nodes)
    await shell.start()

if __name__ == "__main__":
    asyncio.run(main())