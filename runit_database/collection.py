import asyncio
import os
import logging
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urljoin

from dotenv import load_dotenv

from .client import HTTPClient, validate_identifier, validate_filter, validate_columns
from .core import start_subscription

load_dotenv()

logger = logging.getLogger(__name__)


class Collection:
    API_ENDPOINT: str = os.getenv('RUNIT_API_ENDPOINT', '')
    API_KEY: str = os.getenv('RUNIT_API_KEY', '')
    PROJECT_ID: str = os.getenv('RUNIT_PROJECT_ID', '')
    REQUEST_API: str = ''
    WS_ENDPOINT: str = ''
    
    _client: Optional[HTTPClient] = None
    
    def __init__(self, name: str):
        self.name = validate_identifier(name)
        self._ensure_client()
    
    @classmethod
    def _ensure_client(cls) -> HTTPClient:
        if cls._client is None:
            cls._client = HTTPClient()
        return cls._client
    
    @classmethod
    def initialize(cls, api_endpoint: str = '', api_key: str = '', project_id: str = ''):
        cls.API_ENDPOINT = api_endpoint or os.getenv('RUNIT_API_ENDPOINT', '')
        cls.API_KEY = api_key or os.getenv('RUNIT_API_KEY', '')
        cls.PROJECT_ID = project_id or os.getenv('RUNIT_PROJECT_ID', '')
        
        if not cls.API_ENDPOINT:
            raise ValueError("API endpoint is required")
        if not cls.API_KEY:
            raise ValueError("API key is required")
        if not cls.PROJECT_ID:
            raise ValueError("Project ID is required")
        
        cls.REQUEST_API = urljoin(cls.API_ENDPOINT, f'/documents/{cls.PROJECT_ID}/')
        cls.WS_ENDPOINT = urljoin(cls.API_ENDPOINT, '/documents/subscribe/')
        
        cls._client = HTTPClient()
        cls._client.set_headers({'Authorization': f'Bearer {cls.API_KEY}'})
    
    @property
    def _api_base(self) -> str:
        if not self.REQUEST_API:
            self.initialize()
        return self.REQUEST_API + self.name
    
    def count(self, filter: Optional[Dict] = None) -> Dict:
        filter = validate_filter(filter or {})
        data = {'function': 'count', 'filter': filter}
        return self._client.post(self._api_base + '/', json=data)
    
    def select(self, columns: Optional[List[str]] = None, filter: Optional[Dict] = None) -> Dict:
        columns = validate_columns(columns or [])
        filter = validate_filter(filter or {})
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        return self._client.get(self._api_base, params=data)

    def all(self, columns: Optional[List[str]] = None) -> List[Dict]:
        columns = validate_columns(columns or [])
        return self._client.get(self._api_base, params={'columns': columns})

    def get(self, _id: str, columns: Optional[List[str]] = None) -> Dict:
        if not _id:
            raise ValueError("Document ID is required")
        columns = validate_columns(columns or [])
        return self._client.get(f"{self._api_base}/{_id}", params={'columns': columns})

    def find_one(self, _filter: Optional[Dict] = None, columns: Optional[List[str]] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        return self._client.get(self._api_base, params={'filter': _filter, 'columns': columns})

    def find(self, _filter: Optional[Dict] = None, columns: Optional[List[str]] = None) -> List[Dict]:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        return self._client.get(self._api_base + '/', params={'filter': _filter, 'columns': columns})

    def insert_one(self, document: Dict) -> Dict:
        if not isinstance(document, dict):
            raise ValueError("Document must be a dictionary")
        return self._client.post(self._api_base, json={'documents': document})

    def insert_many(self, documents: List[Dict]) -> Dict:
        if not isinstance(documents, list):
            raise ValueError("Documents must be a list")
        if not documents:
            raise ValueError("Documents list cannot be empty")
        return self._client.post(self._api_base, json={'documents': documents})

    def update(self, _filter: Optional[Dict] = None, update: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        if not isinstance(update, dict):
            raise ValueError("Update must be a dictionary")
        data = {'filter': _filter, 'document': update}
        return self._client.put(self._api_base + '/', json=data)

    def remove(self, _filter: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        return self._client.delete(self._api_base, params=_filter)

    def subscribe(self, event: str = 'all', document_id: Optional[str] = None, 
                  callback: Optional[Callable] = None):
        if event not in ('all', 'insert', 'update', 'delete'):
            raise ValueError("Event must be one of: all, insert, update, delete")
        
        def run_subscription():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._subscribe_async(event, document_id, callback))
            finally:
                loop.close()
        
        import threading
        thread = threading.Thread(target=run_subscription, daemon=True)
        thread.start()
    
    async def _subscribe_async(self, event: str, document_id: Optional[str], 
                                callback: Optional[Callable]):
        while True:
            try:
                await start_subscription(
                    self.WS_ENDPOINT,
                    event,
                    self.PROJECT_ID,
                    self.name,
                    document_id,
                    callback
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Subscription error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)
    
    @classmethod
    def close(cls):
        if cls._client:
            cls._client.close()
            cls._client = None
