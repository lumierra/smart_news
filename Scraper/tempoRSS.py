from textacy.preprocess import preprocess_text
# from Database.dbMongo import Database
from bs4 import BeautifulSoup
from tqdm import tqdm
import id_beritagar as indo
import requests
import datetime
import id_aldo
import sys

sys.path.insert(0, '/home/lumierra/smart_news/Database/')
import dbMongo

now = datetime.date.today()

## Load NLP
nlp = id_aldo.load()
nlp_ner = indo.load()

## Load Database Mongo
DB = dbMongo.Database()

## Load Stopword For NLP
stopwords = requests.get("https://raw.githubusercontent.com/masdevid/ID-Stopwords/master/id.stopwords.02.01.2016.txt").text.split("\n")


## Class Scraper Tempo.co
class tempoRSS():
    def __init__(self):
        self.day = now.day
        self.month = now.month
        self.year = now.year

    ## fungsi untuk mendapatkan NER (Named Entity Recogtion) pada Artikel Berita
    def nerMonthly(self, database=None, collection=None, source=None, day=None, month=None, year=None):

        ## mengambil data dari mongoDB
        iQuery = DB.getData(database, collection, source, day, month, year)

        ## query dimasukkan ke dalam array
        iData = []
        for q in iQuery:
            iData.append(q)

        ## proses NLP
        for i in range(len(iData)):
            iText = iData[i]['content'].split('\n')
            iTemp = []
            for t in iText:
                iTemp.append(t + '\n')
            iText = ''.join(iTemp)
            doc = nlp_ner(iText)
            
            ## proses perhitungan NER
            PERSON, ORG, GPE, EVENT, MERK, PRODUCT = 0, 0, 0, 0, 0, 0
            for ent in doc.ents:
                if ent.label_ == 'PERSON':
                    PERSON += 1
                elif ent.label_ == 'ORG':
                    ORG += 1
                elif ent.label_ == 'GPE':
                    GPE += 1
                elif ent.label_ == 'EVENT':
                    EVENT += 1
                elif ent.label_ == 'MERK':
                    MERK += 1
                elif ent.label_ == 'PRODUCT':
                    PRODUCT += 1

            ## proses memberikan hasil banyaknya NER setiap artikel berita
            iData[i]['countNer']['person'] = PERSON
            iData[i]['countNer']['org'] = ORG
            iData[i]['countNer']['gpe'] = GPE
            iData[i]['countNer']['event'] = EVENT
            iData[i]['countNer']['merk'] = MERK
            iData[i]['countNer']['product'] = PRODUCT

            ## proses membuat text biasa ke format html 
            data = []
            for ent in doc.ents:
                data_json = {
                    'text': ent.text,
                    'label': ent.label_
                }
                data.append(data_json)
            unique = {each['text']: each for each in data}.values()
            data = []
            for u in unique:
                data.append(u)

            for d in data:
                iText = iText.replace(d['text'],'''<mark class="{label}-{_id} font-mark transparent style-{label}"> {text} </mark>'''.format(_id=iData[i]['_id'], label=d['label'], text=d['text']))
            iText = ''.join(('''<div class="entities"> ''', iText, ' </div>'))
            iText = iText.split('\n')

            iData[i]['nerContent'] = iText

        return iData
    
    ## fungsi untuk mendapatkan NER (Named Entity Recogtion) pada Artikel Berita
    def getNER(self, database=None, collection=None, source=None):

        ## mengambil data dari mongoDB
        iQuery = DB.get_data(database, collection, source)

        ## query dimasukkan ke dalam array
        iData = []
        for q in iQuery:
            iData.append(q)

        ## proses NLP
        for i in range(len(iData)):
            iText = iData[i]['content'].split('\n')
            iTemp = []
            for t in iText:
                iTemp.append(t + '\n')
            iText = ''.join(iTemp)
            doc = nlp_ner(iText)
            
            ## proses perhitungan NER
            PERSON, ORG, GPE, EVENT, MERK, PRODUCT = 0, 0, 0, 0, 0, 0
            for ent in doc.ents:
                if ent.label_ == 'PERSON':
                    PERSON += 1
                elif ent.label_ == 'ORG':
                    ORG += 1
                elif ent.label_ == 'GPE':
                    GPE += 1
                elif ent.label_ == 'EVENT':
                    EVENT += 1
                elif ent.label_ == 'MERK':
                    MERK += 1
                elif ent.label_ == 'PRODUCT':
                    PRODUCT += 1

            ## proses memberikan hasil banyaknya NER setiap artikel berita
            iData[i]['countNer']['person'] = PERSON
            iData[i]['countNer']['org'] = ORG
            iData[i]['countNer']['gpe'] = GPE
            iData[i]['countNer']['event'] = EVENT
            iData[i]['countNer']['merk'] = MERK
            iData[i]['countNer']['product'] = PRODUCT

            ## proses membuat text biasa ke format html 
            data = []
            for ent in doc.ents:
                data_json = {
                    'text': ent.text,
                    'label': ent.label_
                }
                data.append(data_json)
            unique = {each['text']: each for each in data}.values()
            data = []
            for u in unique:
                data.append(u)

            for d in data:
                iText = iText.replace(d['text'],'''<mark class="{label}-{_id} font-mark transparent style-{label}"> {text} </mark>'''.format(_id=iData[i]['_id'], label=d['label'], text=d['text']))
            iText = ''.join(('''<div class="entities"> ''', iText, ' </div>'))
            iText = iText.split('\n')

            iData[i]['nerContent'] = iText

        return iData

    ## fungsi untuk menghilangkan format artikel berita pada awal kalimat
    def nerText(self, text=None):

        doc = nlp(text)
        count = 0
        for ent in doc.ents:
            if ent.end <= 5:
                count = ent.end_char + 1
            else:
                count = len(text)

        iResult = text[count:].strip()

        return iResult

    ## fungsi ini digunakan untuk mendapatkan konten artikel berita
    def getContent(self, url=None):
        iData = []
        iResponse = requests.get(url).text
        iSoup = BeautifulSoup(iResponse, "html5lib")


        subCategory = iSoup.select('.breadcrumbs > li')[2].text
        img = iSoup.select_one('figure > a')['href']

        iContents = iSoup.select('#isi > p')

        for content in iContents:
            if content.text.strip()[:10] != 'Baca juga:' and content.text.strip()[:5] != 'Baca:':
                iData.append(content.text.strip() + '\n\n')

        ordinaryContent = ''.join(iData)
        ordinaryContent= preprocess_text(ordinaryContent, fix_unicode=True)
        ordinaryContent = self.nerText(ordinaryContent)
        htmlContent = ''.join(iData)
        htmlContent = self.nerText(htmlContent)
        htmlContent = htmlContent.split('\n\n')

        iJson = {
            "subCategory": subCategory,
            "img": img,
            "content": ordinaryContent,
            "contentHTML": htmlContent
        }

        return iJson

    ## fungsi ini digunakan untuk mendapatkan konten artikel berita
    def getContent2(self, iData=None):
        for i in tqdm(range(len(iData)), desc='Get Content'):
            try:
                iTemp = self.getContent(iData[i]['url'])
                iData[i]['content'] = iTemp['content']
                iData[i]['contentHTML'] = iTemp['contentHTML']
                iData[i]['img'] = iTemp['img']
                iData[i]['subCategory'] = iTemp['subCategory']
                iData[i]['description'] = iData[i]['title'] + ' ' + iData[i]['content'][:255] + '....'
            except:
                pass

        return iData

    ## fungsi ini digunakan untuk membersihkan konten di dalam artikel berita
    def cleanContent(self, iData=None):
        for i in tqdm(range(len(iData)), desc='Clean Content'):
            text_stopword = []
            iData[i]['cleanContent'] = preprocess_text(iData[i]['content'], lowercase=True, fix_unicode=True,no_punct=True,no_numbers=True)
            clean_content = iData[i]['cleanContent'].split()

            [text_stopword.append(cc) for cc in clean_content if cc not in stopwords]

            iData[i]['cleanContent'] = ' '.join(text_stopword)

        return iData

    ## fungsi ini digunakan untuk membersihkan/membuang konten yang kosong
    def cleanData(self, iData=None):
        iResult = []
        for data in iData:
            if data['content'] != '':
                iResult.append(data)

        return iResult

    ## fungsi ini digunakan untuk mengambil url dari rss tempo.co
    def getTempoRSS(self):
        tempo = []
        iUrl = '''http://rss.tempo.co/'''
        iResponse = requests.get(iUrl).text
        iSoup = BeautifulSoup(iResponse, "xml")
        iTems = iSoup.select('item')

        for item in iTems:
            link = item.select_one('link').text
            title = item.select_one('title').text
            date = "{}-{}-{}".format(self.day, self.month, self.year)
            # print(link)
            json = {
                'url': link,
                'title': title,
                'date' : date,
                'sumber': 'tempo.co'
            }
            tempo.append(json)
        
        return tempo

    ## fungsi ini digunakan untuk mengambil sumber data dari Tempo.co secara harian
    def getTempo(self, data=None):

        iData = []

        for temp in data:
        
            iJson = {
                'category': '',
                'title': temp['title'],
                'description': '',
                'url': temp['url'],
                'content': '',
                'contentHTML': '',
                'img': '',
                'subCategory': '',
                'publishedAt': temp['date'],
                'source': temp['sumber'],
                'cleanContent': '',
                'nerContent': '',
                'countNer': {
                    'person': 0,
                    'org': 0,
                    'gpe': 0,
                    'event': 0,
                    'merk': 0,
                    'product': 0
                }
            }

            iData.append(iJson)

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara harian
    def iRss(self):
        iData = self.getTempoRSS()
        iData = self.getTempo(iData)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData
        