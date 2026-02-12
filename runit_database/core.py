import asyncio
import logging
from typing import Optional, Callable
import websockets
import json
import time

logger = logging.getLogger(__name__)


async def start_subscription(
    websocket_endpoint: str, 
    event: str, 
    project_id: str, 
    collection: str, 
    document_id: Optional[str] = None, 
    callback: Optional[Callable] = None
):
    if not websocket_endpoint:
        raise ValueError("WebSocket endpoint is required")
    if not project_id:
        raise ValueError("Project ID is required")
    if not collection:
        raise ValueError("Collection name is required")
    
    client_id = str(int(time.time()))
    
    uri = f"{websocket_endpoint}{event}/{client_id}/{project_id}/{collection}"
    if document_id:
        uri = f"{uri}/{document_id}"
    
    uri = uri.replace('http://', 'ws://').replace('https://', 'wss://')
    
    try:
        async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as websocket:
            await websocket.send(json.dumps({"type": "subscriber"}))
            
            async for message in websocket:
                try:
                    response = json.loads(message)
                    if callback:
                        result = callback(response)
                        if asyncio.iscoroutine(result):
                            await result
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in message: {e}")
                except Exception as e:
                    logger.error(f"Error in callback: {e}")
                    
    except websockets.exceptions.ConnectionClosed as e:
        logger.info(f"WebSocket connection closed: {e}")
        raise
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        raise
