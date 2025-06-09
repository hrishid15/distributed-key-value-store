import aiohttp
from aiohttp import web
import json
import asyncio
from .simple_store import SimpleKVStore
from .hash_ring import HashRing

class DistributedNode:
    def __init__(self, node_id, host="localhost", port=8000, replication_factor=3):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.address = f"http://{host}:{port}"
        self.replication_factor = replication_factor
        
        self.store = SimpleKVStore()
        
        self.hash_ring = HashRing()
        self.peers = {}  # node_id -> address
        
        self.session = None
        self.app = self._create_app()
    
    def _create_app(self):
        app = web.Application()
        
        app.router.add_get('/keys/{key}', self.handle_get)
        app.router.add_put('/keys/{key}', self.handle_put)
        app.router.add_delete('/keys/{key}', self.handle_delete)
        
        app.router.add_put('/internal/store/{key}', self.handle_internal_store)
        app.router.add_delete('/internal/delete/{key}', self.handle_internal_delete)
        
        app.router.add_post('/admin/join', self.handle_join)
        app.router.add_post('/admin/notify_join', self.handle_notify_join)
        app.router.add_get('/admin/status', self.handle_status)
        app.router.add_get('/admin/peers', self.handle_get_peers)
        
        return app
    
    async def start(self):
        """Start the node"""
        self.session = aiohttp.ClientSession()
        
        self.hash_ring.add_node(self.node_id)
        self.peers[self.node_id] = self.address
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        print(f"Node {self.node_id} started on {self.address} (replication factor: {self.replication_factor})")
    
    async def stop(self):
        if self.session:
            await self.session.close()
    
    def _get_replica_nodes(self, key):
        return self.hash_ring.get_nodes(key, replicas=self.replication_factor)
    
    def _is_replica_node(self, key):
        replica_nodes = self._get_replica_nodes(key)
        return self.node_id in replica_nodes
    
    async def _replicate_to_peers(self, method, key, value=None, consistency_level='quorum'):
        replica_nodes = self._get_replica_nodes(key)
        
        if consistency_level == 'all':
            required_writes = len(replica_nodes)
            target_nodes = replica_nodes
        elif consistency_level == 'quorum':
            required_writes = (len(replica_nodes) // 2) + 1
            target_nodes = replica_nodes[:required_writes]
        elif consistency_level == 'one':
            required_writes = 1
            target_nodes = replica_nodes[:1]
        else:
            required_writes = (len(replica_nodes) // 2) + 1
            target_nodes = replica_nodes[:required_writes]
        
        print(f"Replicating {method} {key} to {len(target_nodes)}/{len(replica_nodes)} nodes: {target_nodes} (consistency: {consistency_level})")
        
        successful_replicas = 0
        errors = []
        
        for node_id in target_nodes:
            if node_id == self.node_id:
                if method == "PUT":
                    self.store.put(key, value)
                    print(f"Stored {key}={value} locally on {self.node_id}")
                elif method == "DELETE":
                    self.store.delete(key)
                    print(f"Deleted {key} locally on {self.node_id}")
                successful_replicas += 1
            else:
                try:
                    if node_id in self.peers:
                        peer_address = self.peers[node_id]
                        
                        if method == "PUT":
                            url = f"{peer_address}/internal/store/{key}"
                            async with self.session.put(url, json={"value": value}) as resp:
                                if resp.status == 200:
                                    successful_replicas += 1
                                    print(f"Successfully replicated PUT {key} to {node_id}")
                                else:
                                    errors.append(f"{node_id}: HTTP {resp.status}")
                        elif method == "DELETE":
                            url = f"{peer_address}/internal/delete/{key}"
                            async with self.session.delete(url) as resp:
                                if resp.status == 200:
                                    successful_replicas += 1
                                    print(f"Successfully replicated DELETE {key} to {node_id}")
                                else:
                                    errors.append(f"{node_id}: HTTP {resp.status}")
                    else:
                        errors.append(f"{node_id}: Node not in peer list")
                        
                except Exception as e:
                    errors.append(f"{node_id}: {str(e)}")
        
        return successful_replicas, errors, len(target_nodes)
    
    async def _read_from_replicas(self, key):
        replica_nodes = self._get_replica_nodes(key)
        
        print(f"Reading {key} from replica nodes: {replica_nodes}")
        
        if self.node_id in replica_nodes:
            value = self.store.get(key)
            if value is not None:
                print(f"Found {key} locally on {self.node_id}")
                return value, self.node_id
        
        for node_id in replica_nodes:
            if node_id != self.node_id and node_id in self.peers:
                try:
                    peer_address = self.peers[node_id]
                    url = f"{peer_address}/internal/store/{key}"
                    
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            value = data.get('value')
                            if value is not None:
                                print(f"Found {key} on remote node {node_id}")
                                return value, node_id
                except Exception as e:
                    print(f"Error reading from {node_id}: {e}")
                    continue
        
        return None, None
    
    def _check_write_consistency(self, successful_replicas, attempted_replicas, consistency_level):
        if consistency_level == 'all':
            if successful_replicas == attempted_replicas:
                return True, f"All attempted replicas written successfully ({successful_replicas}/{attempted_replicas})"
            else:
                return False, f"Only {successful_replicas}/{attempted_replicas} replicas written (need all)"
        
        elif consistency_level == 'quorum':
            if successful_replicas == attempted_replicas:
                return True, f"Quorum achieved ({successful_replicas}/{attempted_replicas})"
            else:
                return False, f"Quorum not achieved ({successful_replicas}/{attempted_replicas})"
        
        elif consistency_level == 'one':
            if successful_replicas >= 1:
                return True, f"At least one replica written ({successful_replicas}/{attempted_replicas})"
            else:
                return False, "No replicas written successfully"
        
        else:
            if successful_replicas == attempted_replicas:
                return True, f"Consistency achieved ({successful_replicas}/{attempted_replicas})"
            else:
                return False, f"Unknown consistency level '{consistency_level}'"
    
    async def _read_from_all_replicas(self, key):
        replica_nodes = self._get_replica_nodes(key)
        values = []
        source_nodes = []
        
        for node_id in replica_nodes:
            if node_id == self.node_id:
                # Read locally
                value = self.store.get(key)
                if value is not None:
                    values.append(value)
                    source_nodes.append(node_id)
            elif node_id in self.peers:
                # Read from remote node
                try:
                    peer_address = self.peers[node_id]
                    url = f"{peer_address}/internal/store/{key}"
                    
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            value = data.get('value')
                            if value is not None:
                                values.append(value)
                                source_nodes.append(node_id)
                except Exception as e:
                    print(f"Error reading from {node_id}: {e}")
                    continue
        
        return values, source_nodes
    
    async def _read_from_quorum_replicas(self, key):
        replica_nodes = self._get_replica_nodes(key)
        required_reads = (len(replica_nodes) // 2) + 1
        
        values = []
        source_nodes = []
        
        for node_id in replica_nodes:
            if len(values) >= required_reads:
                break
                
            if node_id == self.node_id:
                value = self.store.get(key)
                if value is not None:
                    values.append(value)
                    source_nodes.append(node_id)
            elif node_id in self.peers:
                try:
                    peer_address = self.peers[node_id]
                    url = f"{peer_address}/internal/store/{key}"
                    
                    async with self.session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            value = data.get('value')
                            if value is not None:
                                values.append(value)
                                source_nodes.append(node_id)
                except Exception as e:
                    print(f"Error reading from {node_id}: {e}")
                    continue
        
        return values, source_nodes
    
    async def handle_get(self, request):
        key = request.match_info['key']
        consistency_level = request.query.get('consistency', 'one')
        
        try:
            if consistency_level == 'all':
                values, source_nodes = await self._read_from_all_replicas(key)
                if values:
                    unique_values = set(values)
                    if len(unique_values) == 1:
                        return web.json_response({
                            "key": key, 
                            "value": list(unique_values)[0], 
                            "consistency_level": "all",
                            "source_nodes": source_nodes,
                            "queried_node": self.node_id
                        })
                    else:
                        return web.json_response({
                            "error": "Inconsistent data across replicas",
                            "values_found": list(unique_values),
                            "source_nodes": source_nodes
                        }, status=409)
                else:
                    return web.json_response({"error": "Key not found in any replica"}, status=404)
            
            elif consistency_level == 'quorum':
                values, source_nodes = await self._read_from_quorum_replicas(key)
                if values:
                    from collections import Counter
                    most_common = Counter(values).most_common(1)[0][0]
                    return web.json_response({
                        "key": key, 
                        "value": most_common, 
                        "consistency_level": "quorum",
                        "source_nodes": source_nodes,
                        "queried_node": self.node_id
                    })
                else:
                    return web.json_response({"error": "Key not found in quorum of replicas"}, status=404)
            
            else:
                value, source_node = await self._read_from_replicas(key)
                if value is not None:
                    return web.json_response({
                        "key": key, 
                        "value": value, 
                        "consistency_level": "one",
                        "source_node": source_node,
                        "queried_node": self.node_id
                    })
                else:
                    return web.json_response({"error": "Key not found in any replica"}, status=404)
                    
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    
    async def handle_put(self, request):
        key = request.match_info['key']
        
        try:
            data = await request.json()
            value = data.get('value')
            consistency_level = data.get('consistency', 'quorum')
            
            if value is None:
                return web.json_response({"error": "Missing 'value'"}, status=400)
            
            successful_replicas, errors, attempted_replicas = await self._replicate_to_peers(
                "PUT", key, str(value), consistency_level
            )
            
            total_possible_replicas = len(self._get_replica_nodes(key))
            write_successful, reason = self._check_write_consistency(
                successful_replicas, attempted_replicas, consistency_level
            )
            
            if write_successful:
                return web.json_response({
                    "key": key, 
                    "value": value,
                    "successful_replicas": successful_replicas,
                    "attempted_replicas": attempted_replicas,
                    "total_possible_replicas": total_possible_replicas,
                    "consistency_level": consistency_level,
                    "coordinated_by": self.node_id,
                    "errors": errors if errors else None
                })
            else:
                return web.json_response({
                    "error": f"Write failed: {reason}", 
                    "successful_replicas": successful_replicas,
                    "attempted_replicas": attempted_replicas,
                    "total_possible_replicas": total_possible_replicas,
                    "consistency_level": consistency_level,
                    "errors": errors
                }, status=500)
                
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)
    
    async def handle_delete(self, request):
        key = request.match_info['key']
        
        try:
            consistency_level = request.query.get('consistency', 'quorum')
            successful_replicas, errors, attempted_replicas = await self._replicate_to_peers(
                "DELETE", key, consistency_level=consistency_level
            )
            
            total_possible_replicas = len(self._get_replica_nodes(key))
            delete_successful, reason = self._check_write_consistency(
                successful_replicas, attempted_replicas, consistency_level
            )
            
            if delete_successful:
                return web.json_response({
                    "message": f"Key {key} deleted",
                    "successful_replicas": successful_replicas,
                    "attempted_replicas": attempted_replicas,
                    "total_possible_replicas": total_possible_replicas,
                    "consistency_level": consistency_level,
                    "coordinated_by": self.node_id,
                    "errors": errors if errors else None
                })
            else:
                return web.json_response({
                    "error": f"Delete failed: {reason}",
                    "successful_replicas": successful_replicas,
                    "attempted_replicas": attempted_replicas,
                    "total_possible_replicas": total_possible_replicas,
                    "consistency_level": consistency_level,
                    "errors": errors
                }, status=500)
                
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    
    async def handle_internal_store(self, request):
        key = request.match_info['key']
        
        if request.method == 'GET':
            value = self.store.get(key)
            if value is not None:
                return web.json_response({"key": key, "value": value})
            else:
                return web.json_response({"error": "Key not found"}, status=404)
        
        elif request.method == 'PUT':
            try:
                data = await request.json()
                value = data.get('value')
                if value is None:
                    return web.json_response({"error": "Missing 'value'"}, status=400)
                
                self.store.put(key, str(value))
                return web.json_response({"message": "Stored successfully"})
            except Exception as e:
                return web.json_response({"error": str(e)}, status=400)
    
    async def handle_internal_delete(self, request):
        key = request.match_info['key']
        success = self.store.delete(key)
        return web.json_response({"deleted": success})
    
    async def handle_join(self, request):
        try:
            data = await request.json()
            peer_id = data.get('node_id')
            peer_address = data.get('address')
            
            if not peer_id or not peer_address:
                return web.json_response({"error": "node_id and address required"}, status=400)
            
            print(f"Node {peer_id} requesting to join cluster")
            self.peers[peer_id] = peer_address
            self.hash_ring.add_node(peer_id)
            
            for existing_peer_id, existing_peer_address in self.peers.items():
                if existing_peer_id != peer_id and existing_peer_id != self.node_id:
                    try:
                        await self._notify_peer_about_new_node(
                            existing_peer_address, peer_id, peer_address
                        )
                    except Exception as e:
                        print(f"Failed to notify {existing_peer_id} about new node: {e}")
            
            print(f"Node {peer_id} successfully joined cluster")
            
            return web.json_response({
                "message": "Joined successfully",
                "peers": self.peers
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    
    async def handle_notify_join(self, request):
        try:
            data = await request.json()
            peer_id = data.get('node_id')
            peer_address = data.get('address')
            
            if not peer_id or not peer_address:
                return web.json_response({"error": "node_id and address required"}, status=400)
            
            if peer_id not in self.peers:
                self.peers[peer_id] = peer_address
                self.hash_ring.add_node(peer_id)
                print(f"Learned about new node {peer_id} at {peer_address}")
            
            return web.json_response({"message": "Notification received"})
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)
    
    async def _notify_peer_about_new_node(self, peer_address, new_node_id, new_node_address):
        try:
            async with self.session.post(
                f"{peer_address}/admin/notify_join",
                json={"node_id": new_node_id, "address": new_node_address}
            ) as resp:
                if resp.status != 200:
                    print(f"Failed to notify {peer_address} about {new_node_id}")
        except Exception as e:
            print(f"Error notifying {peer_address}: {e}")
    
    async def handle_get_peers(self, request):
        return web.json_response({"peers": self.peers})
    
    async def handle_status(self, request):
        return web.json_response({
            "node_id": self.node_id,
            "address": self.address,
            "local_keys": self.store.size(),
            "cluster_nodes": list(self.peers.keys()),
            "all_keys_sample": self.store.keys()[:10],
            "hash_ring_nodes": self.hash_ring.get_all_nodes(),
            "replication_factor": self.replication_factor
        })
    
    async def join_cluster(self, peer_address):
        try:
            async with self.session.post(
                f"{peer_address}/admin/join",
                json={"node_id": self.node_id, "address": self.address}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    print(f"Successfully joined cluster via {peer_address}")
                    
                    peers_from_cluster = result.get('peers', {})
                    for peer_id, peer_addr in peers_from_cluster.items():
                        if peer_id != self.node_id:
                            if peer_id not in self.peers:
                                self.peers[peer_id] = peer_addr
                                self.hash_ring.add_node(peer_id)
                                print(f"Added peer {peer_id} at {peer_addr} to my cluster view")
                    
                    return result
                else:
                    print(f"Failed to join cluster: {resp.status}")
                    return None
        except Exception as e:
            print(f"Error joining cluster: {e}")
            return None