import os
import json
from threading import Thread
from typing import Any, Callable, Optional
import asyncio

import requests
from dotenv import load_dotenv
from .collection import Collection
from .core import start_subscription

load_dotenv()

class DocumentType(type):
    API_ENDPOINT = os.getenv('RUNIT_API_ENDPOINT', '')
    API_KEY = os.getenv('RUNIT_API_KEY', '')
    PROJECT_ID = os.getenv('RUNIT_PROJECT_ID', '')
    REQUEST_API = API_ENDPOINT + '/documents/'
    WS_ENDPOINT = REQUEST_API + '/subscribe/'
    HEADERS = {}
    
    def __getattr__(cls, key):
        Collection.initialize(
            cls.API_ENDPOINT,
            cls.API_KEY,
            cls.PROJECT_ID
        )

        return Collection(key)

class Document(metaclass=DocumentType):
    '''
    Class for accessing database api
    '''

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
        Document.REQUEST_API = api_endpoint + '/documents/' + project_id + '/'
        Document.WS_ENDPOINT = api_endpoint + '/documents/subscribe/'
        Document.HEADERS['Authorization'] = f"Bearer {api_key}"
    
    @classmethod
    def count(cls, collection: str = '', filter: dict = {})-> int:
        '''
        Count all documents in collection
        --Document.<collection_name>.count()
        
        @param filter Search filter
        @return int Count of documents based on filter
        '''
        
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
        
        document_api = Document.REQUEST_API + collection
        data = {'columns': columns}
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()

    
    @classmethod
    def get(cls, collection: str, _id: str, columns: list = [])-> dict:
        '''
        Return one document in collection based on filter
        --Document.<collection_name>.find_one()
        
        @param filter Search filter
        @param columns Selected columns in document
        @return Document
        '''

        document_api = Document.REQUEST_API + collection + '/' + _id
        data = {'columns': columns}
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()
    
    @classmethod
    def find_one(cls, collection: str = '', _filter: dict = {}, columns: list = [])-> dict:
        '''
        Return one document in collection based on filter
        --Document.<collection_name>.find_one()
        
        @param filter Search filter
        @param columns Selected columns in document
        @return Document
        '''
        
        document_api = Document.REQUEST_API + collection
        data = {}
        data['filter'] = _filter
        data['columns'] = columns
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def find(cls, collection: str = '', _filter: dict = {}, columns: list = [])-> list[dict]:
        '''
        Return documents in collection based on filter
        --Document.<collection_name>.find()
        
        @param filter Search filter
        @param columns Selected columns in documents
        @return Document results
        '''
        
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['filter'] = _filter
        data['columns'] = columns
        
        req = requests.get(document_api, params=data, headers=Document.HEADERS)
        return req.json()


    @classmethod
    def insert_one(cls, collection: str = '', document: dict = {}):
        '''
        Insert one document into collection

        @param collection Name of collection
        @param document Data to be inserted
        @return insert_result
        '''
        
        document_api = Document.REQUEST_API + collection
        data = {'documents': document}
        
        req = requests.post(document_api, json=data,  headers=Document.HEADERS)
        return req.json()
    
    @classmethod
    def insert_many(cls, collection: str = '', documents: list[dict] = []):
        '''
        Insert list of documents into collection

        @param collection Name of collection
        @param document Data to be inserted
        @return insert_result
        '''
        
        document_api = Document.REQUEST_API + collection
        
        data = {'documents': documents}

        req = requests.post(document_api, data=data,  headers=Document.HEADERS)
        return req.json()

    @classmethod
    def update(cls, collection: str = '', _filter: dict = {},  update: list[dict] = []):
        '''
        Update documents in collection based on filter

        @param collection Name of collection
        @param filer Search filter
        @param update Data to be updated
        @return update_result
        '''
        
        document_api = Document.REQUEST_API + collection + '/'
        data = {}
        data['filter'] = _filter
        data['document'] = update

        req = requests.put(document_api, json=data, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def remove(cls, collection: str = '', _filter: dict = {}):
        '''
        Remove documents in collection based on filter

        @param collection Name of collection
        @param filter Search filter
        @return delete_result
        '''
        
        document_api = Document.REQUEST_API + collection

        req = requests.delete(document_api, params=_filter, headers=Document.HEADERS)
        return req.json()

    @classmethod
    def subscribe(cls, collection: str, event: str = 'all', document_id: Optional[str] = None, _filter: dict = {}, columns: list = [], callback: Optional[Callable] = None):
        loop = asyncio.get_event_loop()
        from threading import Thread

        async def subscribe_async():
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
                    # Subscription was cancelled (e.g., during shutdown)
                    break
                except Exception as e:
                    # Handle other exceptions (e.g., reconnect or log)
                    # print(f"Error in subscription: {e}")
                    await asyncio.sleep(5)  # Retry after a delay

        # Function to run the async code in a separate thread
        def run_async_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(subscribe_async())

        t = Thread(target=run_async_in_thread)
        t.daemon = True
        t.start()

