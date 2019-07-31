from Scraper.tempoScraper import tempoScrapper
from Database.dbMongo import Database
import datetime

## memanggil class tempoScrapper dan class Database
scraperTempo = tempoScrapper()
DB = Database()

##set datetime 
now = datetime.datetime.now().date()
day = now.day
month = now.month
year = now.year

## set variable
iDatabase = 'iData'
iCollection = 'test'
iSource = 'tempo.co'

## list category and name category from Tempo.co
# list_category_tempo = ['nasional', 'pemilu', 'pilpres', 'dunia', 'bisnis', 'bola', 'sport', 'seleb', 'tekno', 'otomotif']
# list_name_category_tempo = ['news', 'news', 'news', 'news', 'bisnis', 'sports', 'sports', 'entertainment', 'tekno', 'otomotif']

list_category_tempo = ['tekno', ]
list_name_category_tempo = ['tekno']


#delete data from mongoDB
DB.delete_dataDaily(iDatabase, iCollection, iSource)

# Get Data
for category, nameCategory in zip(list_category_tempo, list_name_category_tempo):
    iData = scraperTempo.iDaily(category, nameCategory, year, month, day)

    iAttr = []
    for i in range(len(iData)):
        iAttr.append(iData[i])

    DB.insertData(iDatabase, iCollection, iAttr)

iQuery = scraperTempo.getNER(iDatabase, iCollection, iSource)
iData = []
for q in iQuery:
    iData.append(q)
DB.delete_dataDaily(iDatabase, iCollection, iSource)
DB.insertData(iDatabase, iCollection, iData)
