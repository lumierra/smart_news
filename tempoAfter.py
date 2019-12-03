from Scraper.tempoAfter import tempoAfter
from Database.dbMongo import Database
import datetime
import yaml
import os

## OPEN CONFIG FILE YAML
filename_config = os.path.abspath("Config/config.yml")
config = yaml.load(open(filename_config, "r"))

## memanggil class tempoAfter dan class Database
tempoAfter = tempoAfter()
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
        self.after = config['database']['mongo']['after']
        self.port = config['database']['mongo']['port']
        self.iSource = 'tempo.co'
        self.config = config
        self.day = now.day
        self.month = now.month
        self.year = now.year

    def tempoAfter(self):

        DB.deleteRssBefore(self.database, self.after, self.iSource, self.day, self.month, self.year)
        iData = tempoAfter.iDaily()
        iAttr = []
        for i in range(len(iData)): iAttr.append(iData[i])

        DB.insertData(self.database, self.after, iAttr)

iProgram = Tempo()
iProgram.tempoAfter()