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

##CLASS Counter
class Counter():
    ## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.iCounter = config['database']['mongo']['counter']
        self.port = config['database']['mongo']['port']
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

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
            myCollection.insert_one(attr)
            print('Insert Data into MongoDB Successfully')
        except:
            print('Insert Data into Mongod Failed')

    def getData(self):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(self.database)]
        myCollection = myDB["{}".format(self.collection)]

        iQuery = myCollection.find({})

        return iQuery
    
    ## hapus data collection = iCounter
    def deleteData(self):
        myClient = pymongo.MongoClient("mongodb://{}:{}".format(self.host, self.port))
        myDB = myClient["{}".format(self.database)]
        myColection = myDB["{}".format(self.iCounter)]

        iQuery = myColection.remove({})

        return iQuery

    def setCounter(self):
        iData = []
        iQuery = self.getData()

        for q in iQuery:
            iData.append(q)

        news, bisnis, entertainment, sports, tekno, otomotif, health = 0,0,0,0,0,0,0
        for data in iData:
            if data['category'] == 'news': news=news+1
            elif data['category'] == 'bisnis': bisnis=bisnis+1
            elif data['category'] == 'entertainment': entertainment=entertainment+1
            elif data['category'] == 'sports': sports=sports+1
            elif data['category'] == 'tekno': tekno=tekno+1
            elif data['category'] == 'otomotif': otomotif=otomotif+1
            elif data['category'] == 'health': health=health+1
        
        kompas, tempo, liputan = 0,0,0
        for data in iData:
            if data['source'] == 'kompas.com': kompas=kompas+1
            elif data['source'] == 'tempo.co': tempo=tempo+1
            elif data['source'] == 'liputan6.com': liputan=liputan+1

        tanggal = '{}-{}-{}'.format(self.day, self.month, self.year)
        iJson = {
            'tanggal' : tanggal,
            'total' : len(iData),
            'news' : news,
            'bisnis' : bisnis,
            'entertainment' : entertainment,
            'sports' : sports,
            'tekno' : tekno,
            'otomotif' : otomotif,
            'health' : health,
            'kompas' : kompas,
            'tempo' : tempo,
            'liputan' : liputan
        }
        
        return iJson
    
    def execute(self):
        self.deleteData()
        self.insertData(self.database, self.iCounter, self.setCounter())

test = Counter()
test.execute()