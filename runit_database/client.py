import threading
import time
import logging
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class HTTPClient:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.session = requests.Session()
            self._configure_retry()
            self.timeout = 30
            self.initialized = True
    
    def _configure_retry(self):
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def set_headers(self, headers: Dict[str, str]):
        self.session.headers.update(headers)
    
    def set_timeout(self, timeout: int):
        self.timeout = timeout
    
    def get(self, url: str, params: Optional[Dict] = None) -> Dict:
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"GET request failed: {e}")
            return {"error": str(e), "status": "failed"}
    
    def post(self, url: str, json: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict:
        try:
            response = self.session.post(url, json=json, data=data, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"POST request failed: {e}")
            return {"error": str(e), "status": "failed"}
    
    def put(self, url: str, json: Optional[Dict] = None) -> Dict:
        try:
            response = self.session.put(url, json=json, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"PUT request failed: {e}")
            return {"error": str(e), "status": "failed"}
    
    def delete(self, url: str, params: Optional[Dict] = None) -> Dict:
        try:
            response = self.session.delete(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"DELETE request failed: {e}")
            return {"error": str(e), "status": "failed"}
    
    def close(self):
        self.session.close()


def validate_identifier(name: str, max_length: int = 64) -> str:
    import re
    if not name:
        raise ValueError("Identifier cannot be empty")
    name = str(name).strip()
    if len(name) > max_length:
        raise ValueError(f"Identifier too long (max {max_length} characters)")
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(f"Invalid identifier format: {name}")
    return name


def validate_filter(filter_dict: Dict) -> Dict:
    if not isinstance(filter_dict, dict):
        raise ValueError("Filter must be a dictionary")
    validated = {}
    for key, value in filter_dict.items():
        key = validate_identifier(str(key))
        validated[key] = value
    return validated


def validate_columns(columns: List[str]) -> List[str]:
    if not isinstance(columns, list):
        raise ValueError("Columns must be a list")
    return [validate_identifier(col) for col in columns]
