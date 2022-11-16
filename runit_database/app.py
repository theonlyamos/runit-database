import os
import json
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

class DocumentType(type):
    def __getattr__(cls, key):
        cls.COLLECTION = key
        return cls

class Document(metaclass=DocumentType):
    '''
    Class for accessing database api
    '''
    RUNIT_API_ENDPOINT = os.getenv('RUNIT_API_ENDPOINT', '')
    RUNIT_API_KEY = os.getenv('RUNIT_API_KEY', '')
    RUNIT_PROJECT_ID = os.getenv('RUNIT_PROJECT_ID', '')
    REQUEST_API = RUNIT_API_ENDPOINT + '/document/'
    HEADERS = {}
    COLLECTION = ''
    
    def __init__(self):
        pass

    @staticmethod
    def initialize(api_endpoint: str = os.getenv('RUNIT_API_ENDPOINT', ''), 
                 api_key: str = os.getenv('RUNIT_API_KEY', ''), 
                 project_id: str = os.getenv('RUNIT_PROJECT_ID', '')):
        '''
        Initiate project database
        
        @param api_endpoint API ENDPOINT for accessing runit database
        @param api_key API ENDPOINT for accessing runit database
        @param project_id Runit Project ID
        
        @return None
        '''
        Document.API_ENDPOINT = api_endpoint
        Document.API_KEY = api_key
        Document.PROJECT_ID = project_id
        Document.REQUEST_API = api_endpoint + '/document/' + project_id + '/'
        # Document.HEADERS['Authorization'] = f"Bearer {api_key}"
    
    @staticmethod
    def select(collection: str, columns: list = [], filter: dict = {})-> dict:
        '''
        Find selected columns in collection based on paramaters
        
        --Document.documents.find()
        
        @param filter Search filter
        @return Document results
        '''
        document_api = Document.REQUEST_API + collection
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()
    
    @classmethod
    def find(cls, collection: str = '', filter: dict = {}, columns: list = [])-> dict:
        '''
        Return documetns in collection based on filter
        --Document.documents.find()
        
        @param filter Search filter
        @return Document results
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'find'
        data['filter'] = filter
        data['columns'] = columns
        
        req = requests.post(document_api, json=data)
        return req.json()
    
    @classmethod
    def __getattr__(cls, _attr):
        return _attr
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        print(__name, __value)
        pass
