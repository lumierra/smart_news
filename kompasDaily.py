from pymongo.errors import ServerSelectionTimeoutError
from Scraper.kompasScraper import kompasScraper
from Database.dbMongo import Database
import datetime
import pymongo
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## memanggil class tempoScrapper dan class Database
scraperKompas = kompasScraper()
DB = Database()

##set datetime
now = datetime.date.today()

## Class Kompas Daily
class Kompas():
## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.port = config['database']['mongo']['port']
        self.iSource = 'kompas.com'
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    def kompasDaily(self):
        ## list category and name category from Kompas.com
        # list_category_kompas = ['dunia', 'bisnis', 'bola', 'sport', 'seleb', 'tekno', 'otomotif']
        # list_name_category_kompas = ['bisnis', 'sports', 'sports', 'entertainment', 'tekno', 'otomotif']

        list_category_kompas = ['tekno']
        list_name_category_kompas = ['tekno']

        #delete data from mongoDB
        DB.delete_dataDaily(self.database, self.collection, self.iSource)

        # Get Data
        for category, nameCategory in zip(list_category_kompas, list_name_category_kompas):
            iData = scraperKompas.iDaily(category, nameCategory, self.year, self.month, self.day)

            iAttr = []
            for i in range(len(iData)):
                iAttr.append(iData[i])

            DB.insertData(self.database, self.collection, iAttr)

        iQuery = scraperKompas.getNER(self.database, self.collection, self.iSource)
        iData = []
        for q in iQuery:
            iData.append(q)
        DB.delete_dataDaily(self.database, self.collection, self.iSource)
        DB.insertData(self.database, self.collection, iData)

    def kompasMonthly(self):
        try:
            for d in range(0,5):
            
                ## list category and name category from Tempo.co
                # list_category_tempo = ['nasional', 'pemilu', 'pilpres', 'dunia', 'bisnis', 'bola', 'sport', 'seleb', 'tekno', 'otomotif', 'gaya']
                # list_name_category_tempo = ['news', 'news', 'news', 'news', 'bisnis', 'sports', 'sports', 'entertainment', 'tekno', 'otomotif', 'health']

                list_category_tempo = ['nasional', 'dunia', 'bisnis', 'bola', 'sport', 'seleb', 'tekno', 'otomotif', 'gaya']
                list_name_category_tempo = ['news', 'news', 'bisnis', 'sports', 'sports', 'entertainment', 'tekno', 'otomotif', 'health']

                # list_category_tempo = ['gaya', 'tekno']
                # list_name_category_tempo = ['health', 'tekno']

                #delete data from mongoDB
                DB.deleteMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)

                # Get Data
                for category, nameCategory in zip(list_category_tempo, list_name_category_tempo):
                    iData = scraperTempo.iDaily(category, nameCategory, self.year, self.month, d+1)

                    iAttr = []
                    for i in range(len(iData)):
                        iAttr.append(iData[i])

                    DB.insertData(self.database, self.collection, iAttr)
                iQuery = scraperTempo.nerMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)
                iData = []
                for q in iQuery:
                    iData.append(q)
                DB.deleteMonthly(self.database, self.collection, self.iSource, d+1, self.month, self.year)
                DB.insertData(self.database, self.collection, iData)
        except:
            pass

iProgram = Kompas()
iProgram.kompasDaily()