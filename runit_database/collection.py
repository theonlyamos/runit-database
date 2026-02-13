import asyncio
import os
import logging
from typing import Any, Dict, List, Optional, Callable, Union
from urllib.parse import urljoin

from dotenv import load_dotenv

from . import client
from .client import validate_identifier, validate_filter, validate_columns
from .core import start_subscription

load_dotenv()

logger = logging.getLogger(__name__)


class Collection:
    API_ENDPOINT: str = ''
    API_KEY: str = ''
    PROJECT_ID: str = ''
    REQUEST_API: str = ''
    WS_ENDPOINT: str = ''
    _initialized: bool = False
    
    def __init__(self, name: str):
        self.name = validate_identifier(name)
        self._ensure_initialized()
    
    @classmethod
    def _ensure_initialized(cls) -> None:
        if not cls._initialized:
            cls.initialize()
    
    @classmethod
    def initialize(cls, api_endpoint: str = '', api_key: str = '', project_id: str = ''):
        cls.API_ENDPOINT = api_endpoint or os.getenv('RUNIT_API_ENDPOINT', '')
        cls.API_KEY = api_key or os.getenv('RUNIT_API_KEY', '')
        cls.PROJECT_ID = project_id or os.getenv('RUNIT_PROJECT_ID', '')
        
        if not cls.API_ENDPOINT:
            raise ValueError("API endpoint is required. Set RUNIT_API_ENDPOINT env var or pass api_endpoint.")
        if not cls.API_KEY:
            raise ValueError("API key is required. Set RUNIT_API_KEY env var or pass api_key.")
        if not cls.PROJECT_ID:
            raise ValueError("Project ID is required. Set RUNIT_PROJECT_ID env var or pass project_id.")
        
        cls.REQUEST_API = cls.API_ENDPOINT + f'/documents/{cls.PROJECT_ID}/'
        cls.WS_ENDPOINT = cls.API_ENDPOINT + '/documents/subscribe/'

        client.set_headers({'Authorization': f'Bearer {cls.API_KEY}'})
        
        cls._initialized = True
    
    @property
    def _api_base(self) -> str:
        self._ensure_initialized()
        return self.REQUEST_API + self.name
    
    def count(self, filter: Optional[Dict] = None) -> Dict:
        filter = validate_filter(filter or {})
        data = {'function': 'count', 'filter': filter}
        result = client.post(self._api_base + '/', json=data)
        return result if isinstance(result, dict) else {}
    
    def select(self, columns: Optional[List[str]] = None, filter: Optional[Dict] = None) -> Dict:
        columns = validate_columns(columns or [])
        filter = validate_filter(filter or {})
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        result = client.get(self._api_base, params=data)
        return result if isinstance(result, dict) else {}

    def all(self, columns: Optional[List[str]] = None) -> List[Dict]:
        columns = validate_columns(columns or [])
        result = client.get(self._api_base, params={'columns': columns})
        return result if isinstance(result, list) else [result] if isinstance(result, dict) else []

    def get(self, _id: str, columns: Optional[List[str]] = None) -> Dict:
        if not _id:
            raise ValueError("Document ID is required")
        columns = validate_columns(columns or [])
        result = client.get(f"{self._api_base}/{_id}", params={'columns': columns})
        return result if isinstance(result, dict) else {}

    def find_one(self, _filter: Optional[Dict] = None, columns: Optional[List[str]] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        result = client.get(self._api_base, params={'filter': _filter, 'columns': columns})
        return result if isinstance(result, dict) else {}

    def find(self, _filter: Optional[Dict] = None, columns: Optional[List[str]] = None) -> List[Dict]:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        result = client.get(self._api_base + '/', params={'filter': _filter, 'columns': columns})
        return result if isinstance(result, list) else [result] if isinstance(result, dict) else []

    def insert_one(self, document: Dict) -> Dict:
        if not isinstance(document, dict):
            raise ValueError("Document must be a dictionary")
        result = client.post(self._api_base, json={'documents': document})
        return result if isinstance(result, dict) else {}

    def insert_many(self, documents: List[Dict]) -> Dict:
        if not isinstance(documents, list):
            raise ValueError("Documents must be a list")
        if not documents:
            raise ValueError("Documents list cannot be empty")
        result = client.post(self._api_base, json={'documents': documents})
        return result if isinstance(result, dict) else {}

    def update(self, _filter: Optional[Dict] = None, update: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        if not isinstance(update, dict):
            raise ValueError("Update must be a dictionary")
        data = {'filter': _filter, 'document': update}
        result = client.put(self._api_base + '/', json=data)
        return result if isinstance(result, dict) else {}

    def remove(self, _filter: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        result = client.delete(self._api_base, params=_filter)
        return result if isinstance(result, dict) else {}

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
    def reset(cls):
        cls._initialized = False
        cls.API_ENDPOINT = ''
        cls.API_KEY = ''
        cls.PROJECT_ID = ''
        cls.REQUEST_API = ''
        cls.WS_ENDPOINT = ''
