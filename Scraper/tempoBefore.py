from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
import datetime
import sys

sys.path.insert(0, '/home/lumierra/smart_news/Database/')
import dbMongo

## Load Database Mongo
DB = dbMongo.Database()

now = datetime.date.today()

## Class Scraper Tempo.co
class tempoBefore():
    def __init__(self):
        self.day = now.day
        self.month = now.month
        self.year = now.year

    ## fungsi ini digunakan untuk mengambil sumber data dari Tempo.co secara harian
    def tempoDaily(self):

        iData = []
        if self.month <= 9:
            if self.day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/0{}'''.format(self.year, self.month, self.day)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/{}'''.format(self.year, self.month, self.day)
        else:
            if self.day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/0{}'''.format(self.year, self.month, self.day)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/{}'''.format(self.year, self.month, self.day)

        print(iUrl)
        iResponse = requests.get(iUrl).text
        iSoup = BeautifulSoup(iResponse, "html5lib")
        iContents = iSoup.select('.list.list-type-1 > ul > li')

        for i in tqdm(range(len(iContents))):
            tempUrl = iContents[i].select_one('a')['href']
            iTitle = iContents[i].select_one('.title').text
            iDate = iUrl.split('/')[6] + '-' + iUrl.split('/')[5] + '-' + iUrl.split('/')[4]

            iJson = {
                'title': iTitle,
                'url': tempUrl,
                'publishedAt': iDate,
                'source': 'tempo.co',
                'status' : 'yes'
            }

            iData.append(iJson)

        return iData