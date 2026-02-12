import threading
import time
import logging
from typing import Any, Dict, List, Optional, Union
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

_session: Optional[requests.Session] = None
_headers: Dict[str, str] = {}
_timeout: int = 10
_lock = threading.RLock()


def _get_session() -> requests.Session:
    global _session, _headers
    with _lock:
        if _session is None:
            _session = requests.Session()
            retry_strategy = Retry(
                total=2,
                backoff_factor=0.25,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT", "DELETE"],
                raise_on_status=False
            )
            adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=5, pool_maxsize=5)
            _session.mount("http://", adapter)
            _session.mount("https://", adapter)
            if _headers:
                _session.headers.update(_headers)
        return _session


def set_headers(headers: Dict[str, str]) -> None:
    global _headers, _session
    with _lock:
        _headers.update(headers)
        if _session is not None:
            _session.headers.update(headers)


def set_timeout(timeout: int) -> None:
    global _timeout
    _timeout = min(timeout, 30)


def get(url: str, params: Optional[Dict] = None) -> Union[Dict, List]:
    session = _get_session()
    try:
        response = session.get(url, params=params, timeout=_timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"GET request timed out: {url}")
        return {"error": "Request timed out", "status": "failed"}
    except requests.exceptions.RequestException as e:
        logger.error(f"GET request failed: {e}")
        return {"error": str(e), "status": "failed"}


def post(url: str, json: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict:
    session = _get_session()
    try:
        response = session.post(url, json=json, data=data, timeout=_timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"POST request timed out: {url}")
        return {"error": "Request timed out", "status": "failed"}
    except requests.exceptions.RequestException as e:
        logger.error(f"POST request failed: {e}")
        return {"error": str(e), "status": "failed"}


def put(url: str, json: Optional[Dict] = None) -> Dict:
    session = _get_session()
    try:
        response = session.put(url, json=json, timeout=_timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"PUT request timed out: {url}")
        return {"error": "Request timed out", "status": "failed"}
    except requests.exceptions.RequestException as e:
        logger.error(f"PUT request failed: {e}")
        return {"error": str(e), "status": "failed"}


def delete(url: str, params: Optional[Dict] = None) -> Dict:
    session = _get_session()
    try:
        response = session.delete(url, params=params, timeout=_timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"DELETE request timed out: {url}")
        return {"error": "Request timed out", "status": "failed"}
    except requests.exceptions.RequestException as e:
        logger.error(f"DELETE request failed: {e}")
        return {"error": str(e), "status": "failed"}


def close() -> None:
    global _session
    with _lock:
        if _session is not None:
            _session.close()
            _session = None


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
