from pymongo.errors import ServerSelectionTimeoutError
from Scraper.liputanScraper import liputanScraper
from Database.dbMongo import Database
import datetime
import pymongo
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## memanggil class tempoScrapper dan class Database
scraperLiputan = liputanScraper()
DB = Database()

##set datetime
now = datetime.date.today()

## Class Liputan6 Daily
class Liputan():
## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.port = config['database']['mongo']['port']
        self.iSource = 'liputan6.com'
        self.config = config
        self.day = now.day
        self.month = 1
        self.year = now.year

    def liputanDaily(self):
        ## list category and name category from Liputan6
        list_category_liputan = ['news', 'bisnis', 'bola', 'showbiz', 'tekno', 'otomotif']
        list_name_category_liputan = ['news', 'bisnis', 'sports', 'entertainment', 'tekno', 'otomotif']

        # list_category_liputan = ['health', ]
        # list_name_category_liputan = ['health']

        #delete data from mongoDB
        DB.delete_dataDaily(self.database, self.collection, self.iSource)

        # Get Data
        for category, nameCategory in zip(list_category_liputan, list_name_category_liputan):
            iData = scraperLiputan.iDaily(category, nameCategory, self.year, self.month, self.day)

            iAttr = []
            for i in range(len(iData)):
                iAttr.append(iData[i])

            DB.insertData(self.database, self.collection, iAttr)

        iQuery = scraperLiputan.getNER(self.database, self.collection, self.iSource)
        iData = []
        for q in iQuery:
            iData.append(q)
        DB.delete_dataDaily(self.database, self.collection, self.iSource)
        DB.insertData(self.database, self.collection, iData)
    
    def liputanMonthly(self):
        try:
            for d in range(30,31):
            
                ## list category and name category from Liputan 6
                # list_category_liputan = ['news', 'bisnis', 'bola', 'showbiz', 'tekno', 'otomotif', 'health']
                # list_name_category_liputan = ['news', 'bisnis', 'sports', 'entertainment', 'tekno', 'otomotif', 'health']

                list_category_liputan = ['tekno', 'otomotif', 'health']
                list_name_category_liputan = ['tekno', 'otomotif', 'health']

                #delete data from mongoDB
                DB.deleteMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)

                # Get Data
                for category, nameCategory in zip(list_category_liputan, list_name_category_liputan):
                    iData = scraperLiputan.iDaily(category, nameCategory, self.year, self.month, d+1)

                    iAttr = []
                    for i in range(len(iData)):
                        iAttr.append(iData[i])

                    DB.insertData(self.database, self.collection, iAttr)
                iQuery = scraperLiputan.nerMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)
                iData = []
                for q in iQuery:
                    iData.append(q)
                DB.deleteMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)
                DB.insertData(self.database, self.collection, iData)
        except:
            pass

iProgram = Liputan()
# iProgram.liputanDaily()
iProgram.liputanMonthly()