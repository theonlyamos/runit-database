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
        Document.HEADERS['Authorization'] = f"Bearer {api_key}"
    
    @classmethod
    def count(cls, collection: str = '', filter: dict = {})-> int:
        '''
        Count all documents in collection
        --Document.<collection_name>.count()
        
        @param filter Search filter
        @return int Count of documents based on filter
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'count'
        data['filter'] = filter
        
        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()
        
    @staticmethod
    def select(collection: str, columns: list = [], filter: dict = {})-> dict:
        '''
        Find selected columns in collection based on filter
        --Document.<collection_name>.select()
        
        @param filter Search filter
        @return Document results
        '''
        document_api = Document.REQUEST_API + collection
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def all(cls, collection: str = '', columns: list = [])-> list[dict]:
        '''
        Return all documents in collection
        --Document.<collection_name>.all()
        
        @param columns Selected columns in documents
        @return Document results
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'all'
        data['columns'] = columns
        
        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()

    
    @classmethod
    def find_one(cls, collection: str = '', filter: dict = {}, columns: list = [])-> dict:
        '''
        Return one document in collection based on filter
        --Document.<collection_name>.find_one()
        
        @param filter Search filter
        @param columns Selected columns in document
        @return Document
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'find'
        data['filter'] = filter
        data['columns'] = columns
        
        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def find(cls, collection: str = '', filter: dict = {}, columns: list = [])-> list[dict]:
        '''
        Return documents in collection based on filter
        --Document.<collection_name>.find()
        
        @param filter Search filter
        @param columns Selected columns in documents
        @return Document results
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'find'
        data['filter'] = filter
        data['columns'] = columns
        
        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()


    @classmethod
    def insert_one(cls, collection: str = '', document: dict = {}):
        '''
        Insert one document into collection

        @param collection Name of collection
        @param document Data to be inserted
        @return insert_result
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        
        data = {}
        data['function'] = 'insert'
        data['document'] = document

        req = requests.post(document_api, json=data,  headers=Document.HEADERS)
        return req.json()
    
    @classmethod
    def insert_many(cls, collection: str = '', document: list[dict] = []):
        '''
        Insert list of documents into collection

        @param collection Name of collection
        @param document Data to be inserted
        @return insert_result
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        
        data = {}
        data['function'] = 'insert'
        data['document'] = document

        req = requests.post(document_api, json=data,  headers=Document.HEADERS)
        return req.json()

    @classmethod
    def update(cls, collection: str = '', filter: dict = {},  update: list[dict] = []):
        '''
        Update documents in collection based on filter

        @param collection Name of collection
        @param filer Search filter
        @param update Data to be updated
        @return update_result
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['function'] = 'update'
        data['filter'] = filter
        data['update'] = update

        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def remove(cls, collection: str = '', filter: dict = {}):
        '''
        Remove documents in collection based on filter

        @param collection Name of collection
        @param filter Search filter
        @return delete_result
        '''
        collection = collection if collection else cls.COLLECTION
        document_api = Document.REQUEST_API + collection + '/'

        req = requests.post(document_api, json=data, headers=Document.HEADERS)
        return req.json()


