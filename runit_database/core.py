from typing import Optional, Callable
import websockets
import json
import time

async def start_subscription(websocket_endpoint, event: str, project_id: str, collection: str, document_id: Optional[str] = None, callback: Optional[Callable] = None):
    """Start a subscription to the specified collection."""
    
    client_id = int(time.time())

    uri: str = f"{websocket_endpoint}{event}/{client_id}/{project_id}/{collection}"
    uri = f"{uri}/{document_id}" if document_id else uri
    
    uri = uri.replace('http', 'ws')

    async with websockets.connect(uri) as websocket:
        await websocket.send(json.dumps({"type": "subscriber"}))
        # Wait for incoming messages
        while True:
            try:
                response = await websocket.recv()
                response = json.loads(response)
                
                if callback:
                    return callback(response)
            except websockets.exceptions.ConnectionClosed:
                print("Connection with server closed")
                break
