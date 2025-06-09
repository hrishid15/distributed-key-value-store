from aiohttp import web
import json
from .simple_store import SimpleKVStore

class HTTPKVStore:
    def __init__(self, port=8000):
        self.store = SimpleKVStore()
        self.port = port
        self.app = self._create_app()
    
    def _create_app(self):
        app = web.Application()
        
        app.router.add_get('/keys/{key}', self.handle_get)
        app.router.add_put('/keys/{key}', self.handle_put)
        app.router.add_delete('/keys/{key}', self.handle_delete)
        app.router.add_get('/stats', self.handle_stats)
        
        return app
    
    async def handle_get(self, request):
        key = request.match_info['key']
        value = self.store.get(key)
        
        if value is not None:
            return web.json_response({"key": key, "value": value})
        else:
            return web.json_response({"error": "Key not found"}, status=404)
    
    async def handle_put(self, request):
        key = request.match_info['key']
        
        try:
            data = await request.json()
            value = data.get('value')
            
            if value is None:
                return web.json_response({"error": "Missing 'value' in request"}, status=400)
            
            self.store.put(key, str(value))
            return web.json_response({"key": key, "value": value})
            
        except Exception as e:
            return web.json_response({"error": f"Invalid JSON: {str(e)}"}, status=400)
    
    async def handle_delete(self, request):
        key = request.match_info['key']
        success = self.store.delete(key)
        
        if success:
            return web.json_response({"message": f"Key '{key}' deleted"})
        else:
            return web.json_response({"error": "Key not found"}, status=404)
    
    async def handle_stats(self, request):
        return web.json_response({
            "total_keys": self.store.size(),
            "keys": self.store.keys()
        })
    
    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()
        
        print(f"KV Store server running on http://localhost:{self.port}")
        print("Routes:")
        print("  GET    /keys/{key}     - Get a value")
        print("  PUT    /keys/{key}     - Set a value")
        print("  DELETE /keys/{key}     - Delete a key")
        print("  GET    /stats          - Get statistics")