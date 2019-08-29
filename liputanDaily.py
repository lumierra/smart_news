from Scraper.liputanScraper import liputanScraper
from Database.dbMongo import Database
import datetime

## memanggil class tempoScrapper dan class Database
scraperLiputan = liputanScraper()
DB = Database()

##set datetime 
now = datetime.datetime.now().date()
day = now.day
month = now.month
year = now.year

## set variable
iDatabase = 'iData'
iCollection = 'test'
iSource = 'liputan6.com'

## list category and name category from Tempo.co
# list_category_liputan = ['news', 'bisnis', 'bola', 'showbiz', 'tekno', 'otomotif']
# list_name_category_liputan = ['news', 'bisnis', 'sports', 'entertainment', 'tekno', 'otomotif']

list_category_tempo = ['bola', ]
list_name_category_tempo = ['sports']


#delete data from mongoDB
DB.delete_dataDaily(iDatabase, iCollection, iSource)

# Get Data
for category, nameCategory in zip(list_category_tempo, list_name_category_tempo):
    iData = scraperLiputan.iDaily(category, nameCategory, year, month, day)

    iAttr = []
    for i in range(len(iData)):
        iAttr.append(iData[i])

    DB.insertData(iDatabase, iCollection, iAttr)

iQuery = scraperLiputan.getNER(iDatabase, iCollection, iSource)
iData = []
for q in iQuery:
    iData.append(q)
DB.delete_dataDaily(iDatabase, iCollection, iSource)
DB.insertData(iDatabase, iCollection, iData)
