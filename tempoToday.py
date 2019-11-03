from Scraper.tempoToday import tempoToday
from Database.dbMongo import Database
import datetime
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## memanggil class tempoScrapper dan class Database
tempoToday = tempoToday()
DB = Database()

##set datetime
now = datetime.date.today()

## Class Tempo Daily
class Tempo():
## fungsi untuk menginisialisasi default dalam class Database
    def __init__(self):
        self.host = config['database']['mongo']['host']
        self.database = config['database']['mongo']['database']
        self.collection = config['database']['mongo']['collection']
        self.before = config['database']['mongo']['before']
        self.port = config['database']['mongo']['port']
        self.iSource = 'tempo.co'
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    def tempoToday(self):

        DB.deleteRssBefore(self.database, self.before, self.iSource, self.day, self.month, self.year)
        iData = tempoToday.iDaily()
        iAttr = []
        for i in range(len(iData)): iAttr.append(iData[i])

        DB.insertData(self.database, self.before, iAttr)

        iQuery = tempoToday.getNER(self.database, self.before, self.iSource)
        iData = []
        for q in iQuery: iData.append(q)
        DB.deleteRssBefore(self.database, self.before, self.iSource, self.day, self.month, self.year)
        DB.insertData(self.database, self.before, iData)

iProgram = Tempo()
iProgram.tempoToday()