from Scraper.kompasToday import kompasToday
from Database.dbMongo import Database
import datetime
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## memanggil class tempoScrapper dan class Database
kompasToday = kompasToday()
DB = Database()

##set datetime
now = datetime.date.today()

## Class Tempo Daily
class Kompas():
## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.before = config['database']['mongo']['before']
        self.port = config['database']['mongo']['port']
        self.iSource = 'kompas.com'
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    def DailyRSS(self):

        DB.deleteRssBefore(self.database, 'temp', self.iSource, self.day, self.month, self.year)
        iData = kompasToday.iDaily()
        iAttr = []
        for i in range(len(iData)):
            iAttr.append(iData[i])

        DB.insertData(self.database, 'temp', iAttr)

        # iQuery = tempoToday.getNER(self.database, 'temp', self.iSource)
        # iData = []
        # for q in iQuery:
        #     iData.append(q)
        # DB.delete_dataDaily(self.database, 'temp', self.iSource)
        # DB.insertData(self.database, 'temp', iData)

iProgram = Kompas()
iProgram.DailyRSS()