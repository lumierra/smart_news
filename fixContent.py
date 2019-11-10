import pymongo
import id_aldo
from tqdm import tqdm
from textacy.preprocess import  preprocess_text

class Content():
    def __init__(self):
        self

    def insertData(self, attr=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.dataset

        try:
            collection.insert_many(attr)
            print('Insert Data into MongoDB Successfully')
        except:
            print('Insert Data into Mongod Failed')

    def deleteData(self, category=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.dataset

        iQuery = collection.remove({
            'category' : '{}'.format(category)
        })

        return iQuery
    
    def getData(self, category=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.dataset

        iQuery = collection.find({
            'category' : '{}'.format(category)
        })

        return iQuery

    def cleanContent(self):
        iData = []
        categories = ['news', 'bisnis', 'sports', 'entertainment', 'tekno', 'otomotif', 'health']
        


