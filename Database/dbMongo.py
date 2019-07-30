from pymongo.errors import ServerSelectionTimeoutError
import datetime
import pymongo
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

##SET DATETIME
now = datetime.date.today()

##CLASS DATABASE
class Database():
    ## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.port = config['database']['mongo']['port']
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    ## fungsi untuk mengecek koneksi mongoDB
    def test_connection(self):
        client = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port), serverSelectionTimeoutMS=10, connectTimeoutMS=20000)

        try:
            print(client.server_info())
        except ServerSelectionTimeoutError:
            print("Server is down.")

    ## fungsi untuk menginput/menginsert data ke mongoDB
    def insertData(self, database=None, collection=None, attr=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        try:
            myCollection.insert_many(attr)
            print('Insert Data into MongoDB Successfully')
        except:
            print('Insert Data into Mongod Failed')

    ## fungsi untuk mengambil data dari MongoDB
    def get_data(self, database=None, collection=None, source=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        if self.month <= 9:
            if self.day <= 9:
                iQuery = myCollection.find({
                    'publishedAt': '0{}-0{}-{}'.format(self.day, self.month, self.year),
                    'source' : source
                })
            else:
                iQuery = myCollection.find({
                    'publishedAt': '{}-0{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })
        else:
            if self.day <= 9:
                iQuery = myCollection.find({
                    'publishedAt': '0{}-{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })
            else:
                iQuery = myCollection.find({
                    'publishedAt': '{}-{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })

        return iQuery

    ## fungsi ini digunakan untuk menghapus data harian berdasarkan sourcenya
    def delete_dataDaily(self, database=None, collection=None, source=None):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(database)]
        myCollection = myDB["{}".format(collection)]

        if self.month <= 9:
            if self.day <= 9:
                iQuery = myCollection.remove({
                    'publishedAt': '0{}-0{}-{}'.format(self.day, self.month, self.year),
                    'source' : source
                })
            else:
                iQuery = myCollection.remove({
                    'publishedAt': '{}-0{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })
        else:
            if self.day <= 9:
                iQuery = myCollection.remove({
                    'publishedAt': '0{}-{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })
            else:
                iQuery = myCollection.remove({
                    'publishedAt': '{}-{}-{}'.format(self.day, self.month, self.year),
                    'source': source
                })

        return iQuery