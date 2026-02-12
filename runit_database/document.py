import asyncio
import os
import logging
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urljoin

from dotenv import load_dotenv

from .client import HTTPClient, validate_identifier, validate_filter, validate_columns
from .collection import Collection
from .core import start_subscription

load_dotenv()

logger = logging.getLogger(__name__)


class DocumentType(type):
    API_ENDPOINT: str = os.getenv('RUNIT_API_ENDPOINT', '')
    API_KEY: str = os.getenv('RUNIT_API_KEY', '')
    PROJECT_ID: str = os.getenv('RUNIT_PROJECT_ID', '')
    
    def __getattr__(cls, key: str):
        Collection.initialize(cls.API_ENDPOINT, cls.API_KEY, cls.PROJECT_ID)
        return Collection(key)


class Document(metaclass=DocumentType):
    API_ENDPOINT: str = os.getenv('RUNIT_API_ENDPOINT', '')
    API_KEY: str = os.getenv('RUNIT_API_KEY', '')
    PROJECT_ID: str = os.getenv('RUNIT_PROJECT_ID', '')
    REQUEST_API: str = ''
    WS_ENDPOINT: str = ''
    
    _client: Optional[HTTPClient] = None
    
    @classmethod
    def _ensure_client(cls) -> HTTPClient:
        if cls._client is None:
            cls._client = HTTPClient()
        return cls._client
    
    @staticmethod
    def initialize(api_endpoint: str = '', api_key: str = '', project_id: str = ''):
        Document.API_ENDPOINT = api_endpoint or os.getenv('RUNIT_API_ENDPOINT', '')
        Document.API_KEY = api_key or os.getenv('RUNIT_API_KEY', '')
        Document.PROJECT_ID = project_id or os.getenv('RUNIT_PROJECT_ID', '')
        
        if not Document.API_ENDPOINT:
            raise ValueError("API endpoint is required")
        if not Document.API_KEY:
            raise ValueError("API key is required")
        if not Document.PROJECT_ID:
            raise ValueError("Project ID is required")
        
        Document.REQUEST_API = urljoin(Document.API_ENDPOINT, f'/documents/{Document.PROJECT_ID}/')
        Document.WS_ENDPOINT = urljoin(Document.API_ENDPOINT, '/documents/subscribe/')
        
        Document._client = HTTPClient()
        Document._client.set_headers({'Authorization': f'Bearer {Document.API_KEY}'})
        
        Collection.initialize(Document.API_ENDPOINT, Document.API_KEY, Document.PROJECT_ID)
    
    @classmethod
    def _api_base(cls, collection: str) -> str:
        if not cls.REQUEST_API:
            cls.initialize()
        return cls.REQUEST_API + validate_identifier(collection)
    
    @classmethod
    def count(cls, collection: str, filter: Optional[Dict] = None) -> Dict:
        filter = validate_filter(filter or {})
        data = {'function': 'count', 'filter': filter}
        return cls._ensure_client().post(cls._api_base(collection) + '/', json=data)
    
    @classmethod
    def select(cls, collection: str, columns: Optional[List[str]] = None, 
               filter: Optional[Dict] = None) -> Dict:
        columns = validate_columns(columns or [])
        filter = validate_filter(filter or {})
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        return cls._ensure_client().get(cls._api_base(collection), params=data)

    @classmethod
    def all(cls, collection: str, columns: Optional[List[str]] = None) -> List[Dict]:
        columns = validate_columns(columns or [])
        return cls._ensure_client().get(cls._api_base(collection), params={'columns': columns})

    @classmethod
    def get(cls, collection: str, _id: str, columns: Optional[List[str]] = None) -> Dict:
        if not _id:
            raise ValueError("Document ID is required")
        columns = validate_columns(columns or [])
        return cls._ensure_client().get(f"{cls._api_base(collection)}/{_id}", params={'columns': columns})

    @classmethod
    def find_one(cls, collection: str, _filter: Optional[Dict] = None, 
                 columns: Optional[List[str]] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        return cls._ensure_client().get(cls._api_base(collection), 
                                        params={'filter': _filter, 'columns': columns})

    @classmethod
    def find(cls, collection: str, _filter: Optional[Dict] = None, 
             columns: Optional[List[str]] = None) -> List[Dict]:
        _filter = validate_filter(_filter or {})
        columns = validate_columns(columns or [])
        return cls._ensure_client().get(cls._api_base(collection) + '/', 
                                        params={'filter': _filter, 'columns': columns})

    @classmethod
    def insert_one(cls, collection: str, document: Dict) -> Dict:
        if not isinstance(document, dict):
            raise ValueError("Document must be a dictionary")
        return cls._ensure_client().post(cls._api_base(collection), json={'documents': document})

    @classmethod
    def insert_many(cls, collection: str, documents: List[Dict]) -> Dict:
        if not isinstance(documents, list):
            raise ValueError("Documents must be a list")
        if not documents:
            raise ValueError("Documents list cannot be empty")
        return cls._ensure_client().post(cls._api_base(collection), json={'documents': documents})

    @classmethod
    def update(cls, collection: str, _filter: Optional[Dict] = None, 
               update: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        if not isinstance(update, dict):
            raise ValueError("Update must be a dictionary")
        data = {'filter': _filter, 'document': update}
        return cls._ensure_client().put(cls._api_base(collection) + '/', json=data)

    @classmethod
    def remove(cls, collection: str, _filter: Optional[Dict] = None) -> Dict:
        _filter = validate_filter(_filter or {})
        return cls._ensure_client().delete(cls._api_base(collection), params=_filter)

    @classmethod
    def subscribe(cls, collection: str, event: str = 'all', document_id: Optional[str] = None,
                  callback: Optional[Callable] = None):
        if event not in ('all', 'insert', 'update', 'delete'):
            raise ValueError("Event must be one of: all, insert, update, delete")
        
        def run_subscription():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    cls._subscribe_async(collection, event, document_id, callback)
                )
            finally:
                loop.close()
        
        import threading
        thread = threading.Thread(target=run_subscription, daemon=True)
        thread.start()
    
    @classmethod
    async def _subscribe_async(cls, collection: str, event: str, 
                                document_id: Optional[str], callback: Optional[Callable]):
        while True:
            try:
                await start_subscription(
                    cls.WS_ENDPOINT,
                    event,
                    cls.PROJECT_ID,
                    collection,
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
