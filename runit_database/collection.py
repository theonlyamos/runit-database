import os
import json
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

class Collection():
    '''
    Class for accessing database api
    '''
    RUNIT_API_ENDPOINT = os.getenv('RUNIT_API_ENDPOINT', '')
    RUNIT_API_KEY = os.getenv('RUNIT_API_KEY', '')
    RUNIT_PROJECT_ID = os.getenv('RUNIT_PROJECT_ID', '')
    REQUEST_API = RUNIT_API_ENDPOINT + '/document/'
    HEADERS = {}
    
    def __init__(self, name):
        self.name = name

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
        Collection.API_ENDPOINT = api_endpoint
        Collection.API_KEY = api_key
        Collection.PROJECT_ID = project_id
        Collection.REQUEST_API = api_endpoint + '/documents/' + project_id + '/'
        Collection.HEADERS['Authorization'] = f"Bearer {api_key}"
    
    def count(self, filter: dict = {})-> int:
        '''
        Count all documents in collection
        --Document.<collection_name>.count()
        
        @param filter Search filter
        @return int Count of documents based on filter
        '''
        
        document_api = Collection.REQUEST_API + self.name +'/'
        data = {}
        data['function'] = 'count'
        data['filter'] = filter
        
        req = requests.post(document_api, json=data, headers=Collection.HEADERS)
        return req.json()
        
    def select(self, columns: list = [], filter: dict = {})-> dict:
        '''
        Find selected columns in collection based on filter
        --Document.<collection_name>.select()
        
        @param filter Search filter
        @return Document results
        '''
        document_api = Collection.REQUEST_API + self.name
        data = {'function': 'select', 'columns': columns, 'filter': filter}
        
        req = requests.get(document_api, params=data, headers=Collection.HEADERS)
        return req.json()

    def all(self, columns: list = [])-> list[dict]:
        '''
        Return all documents in collection
        --Document.<collection_name>.all()
        
        @param columns Selected columns in documents
        @return Document results
        '''
        
        document_api = Collection.REQUEST_API + self.name
        data = {'columns': columns}
        
        req = requests.get(document_api, params=data, headers=Collection.HEADERS)
        return req.json()

    
    def get(self, _id: str, columns: list = [])-> dict:
        '''
        Return one document in collection based on filter
        --Document.<collection_name>.find_one()
        
        @param filter Search filter
        @param columns Selected columns in document
        @return Document
        '''
        
        document_api = Collection.REQUEST_API + self.name +'/' + _id
        data = {'columns': columns}
        
        req = requests.get(document_api, params=data, headers=Collection.HEADERS)
        return req.json()
    

    def find_one(self, _filter: dict = {}, columns: list = [])-> dict:
        '''
        Return one document in collection based on filter
        --Document.<collection_name>.find_one()
        
        @param filter Search filter
        @param columns Selected columns in document
        @return Document
        '''
        
        document_api = Collection.REQUEST_API + self.name
        data = {}
        data['filter'] = _filter
        data['columns'] = columns
        
        req = requests.get(document_api, params=data, headers=Collection.HEADERS)
        return req.json()


    def find(self, _filter: dict = {}, columns: list = [])-> list[dict]:
        '''
        Return documents in collection based on filter
        --Document.<collection_name>.find()
        
        @param filter Search filter
        @param columns Selected columns in documents
        @return Document results
        '''
        
        document_api = Collection.REQUEST_API + self.name +'/'
        data = {}
        data['filter'] = _filter
        data['columns'] = columns
        
        req = requests.get(document_api, params=data, headers=Collection.HEADERS)
        return req.json()



    def insert_one(self, document: dict = {}):
        '''
        Insert one document into collection

        @param Name of collection
        @param Data to be inserted
        @return insert_result
        '''
        
        document_api = Collection.REQUEST_API + self.name
        data = {'documents': document}
        
        req = requests.post(document_api, json=data,  headers=Collection.HEADERS)
        return req.json()
    

    def insert_many(self, documents: list[dict] = []):
        '''
        Insert list of documents into collection

        @param Name of collection
        @param Data to be inserted
        @return insert_result
        '''
        
        document_api = Collection.REQUEST_API + self.name
        
        data = {'documents': documents}

        req = requests.post(document_api, data=data,  headers=Collection.HEADERS)
        return req.json()


    def update(self, _filter: dict = {},  update: list[dict] = []):
        '''
        Update documents in collection based on filter

        @param collection Name of collection
        @param filer Search filter
        @param update Data to be updated
        @return update_result
        '''
        
        document_api = Collection.REQUEST_API + self.name +'/'
        data = {}
        data['filter'] = _filter
        data['document'] = update

        req = requests.put(document_api, json=data, headers=Collection.HEADERS)
        return req.json()


    def remove(self, _filter: dict = {}):
        '''
        Remove documents in collection based on filter

        @param collection Name of collection
        @param filter Search filter
        @return delete_result
        '''
        
        document_api = Collection.REQUEST_API + self.name

        req = requests.delete(document_api, params=_filter, headers=Collection.HEADERS)
        return req.json()