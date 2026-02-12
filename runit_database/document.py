import asyncio
import os
import logging
from typing import Any, Dict, List, Optional, Callable, Union
from urllib.parse import urljoin

from dotenv import load_dotenv

from . import client
from .client import validate_identifier, validate_filter, validate_columns
from .collection import Collection
from .core import start_subscription

load_dotenv()

logger = logging.getLogger(__name__)


class DocumentType(type):
    API_ENDPOINT: str = ''
    API_KEY: str = ''
    PROJECT_ID: str = ''
    
    def __getattr__(cls, key: str) -> Collection:
        if not Collection._initialized:
            Collection.initialize(cls.API_ENDPOINT, cls.API_KEY, cls.PROJECT_ID)
        return Collection(key)


class Document(metaclass=DocumentType):
    API_ENDPOINT: str = ''
    API_KEY: str = ''
    PROJECT_ID: str = ''
    
    @staticmethod
    def initialize(api_endpoint: str = '', api_key: str = '', project_id: str = ''):
        Document.API_ENDPOINT = api_endpoint or os.getenv('RUNIT_API_ENDPOINT', '')
        Document.API_KEY = api_key or os.getenv('RUNIT_API_KEY', '')
        Document.PROJECT_ID = project_id or os.getenv('RUNIT_PROJECT_ID', '')
        
        # Sync with metaclass so __getattr__ uses correct values
        DocumentType.API_ENDPOINT = Document.API_ENDPOINT
        DocumentType.API_KEY = Document.API_KEY
        DocumentType.PROJECT_ID = Document.PROJECT_ID
        
        Collection.initialize(Document.API_ENDPOINT, Document.API_KEY, Document.PROJECT_ID)
    
    @staticmethod
    def reset():
        Document.API_ENDPOINT = ''
        Document.API_KEY = ''
        Document.PROJECT_ID = ''
        
        # Sync reset with metaclass
        DocumentType.API_ENDPOINT = ''
        DocumentType.API_KEY = ''
        DocumentType.PROJECT_ID = ''
        
        Collection.reset()
