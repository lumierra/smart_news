from collections import Counter
from tqdm import tqdm
import id_beritagar
import datetime
import pymongo
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## set datetime
now = datetime.date.today()
year = now.year
month = now.month
day = now.day

nlp = id_beritagar.load()

class Entity(object):
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.entity = config['database']['mongo']['entity']
        self.port = config['database']['mongo']['port']
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    ## insert top entitas ke database topEntity
    def insertTopEntity(self, database=None, collection=None, attr=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        try:
            insert_db = myCollection.insert(attr)
            print('Insert Data into MongoDB Succesfully')
        except:
            print('Insert Data into MongoDB Failed')

    ## ambil data dari database topEntity
    def getQuery(self, database=None, collection=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        if self.month <= 9:
            if self.day <= 9:
                iQuery = myCollection.find({
                    'publishedAt': '0{}-0{}-{}'.format(self.day, self.month, self.year)
                })
            else:
                iQuery = myCollection.find({
                    'publishedAt': '{}-0{}-{}'.format(self.day, self.month, self.year)
                })
        else:
            if self.day <= 9:
                iQuery = myCollection.find({
                    'publishedAt': '0{}-{}-{}'.format(self.day, self.month, self.year)
                })
            else:
                iQuery = myCollection.find({
                    'publishedAt': '{}-{}-{}'.format(self.day, self.month, self.year)
                })

        iData = []
        for q in iQuery:
            iData.append(q)

        return iData

    ## delete data top entity
    def deleteDataDaily(self, database=None, collection=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        iQuery = myCollection.remove({
            'publishedAt' : '{}-{}-{}'.format(self.day, self.month, self.year)
        })

        return iQuery

    ## fungsi untuk mencari sebuah entitas di dalam artikel berita
    def getNer(self, iData=None):
        data = []
        for ad in tqdm(iData, desc='Top Entity'):
            doc = nlp(ad['content'])
            for ent in doc.ents:
                data.append(ent.text)

        for i in range(len(data)):
            if '\n' in data[i]:
                data[i] = data[i].replace('\n', '')

        return data

    ## fungsi ini untuk menghitung jumlah NER yang didapat
    def getCounter(self, iData):
        data = []
        for ad in iData:
            if ad != '':
                data.append(ad)

        return data

    ## fungsi ini digunakan untuk menyimpan data ke dalam format json
    def setJson(self, iData):
        iJson = {
            'publishedAt': '{}-{}-{}'.format(self.day, self.month, self.year),
            'top_ner': iData
        }

        return  iJson

    ## proses melakukan topEntity
    def topEntity(self):
        self.deleteDataDaily(self.database, self.entity)
        entity = self.getQuery(self.database, self.collection)
        entity = self.getNer(entity)
        entity = self.getCounter(entity)
        entity = Counter(entity)
        entity = entity.most_common(30)
        entity = self.setJson(entity)

        self.insertTopEntity(self.database, self.entity, entity)

