from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from textacy.preprocess import preprocess_text
from sklearn.externals import joblib
from bs4 import BeautifulSoup
from tqdm import tqdm
import id_beritagar as indo
import requests
import id_aldo
import sys

sys.path.insert(0, '/home/lumierra/smart_news/Database/')
import dbMongo

## Load NLP
nlp = id_aldo.load()
nlp_ner = indo.load()

# create stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()

## Load Database Mongo
DB = dbMongo.Database()

## Load Stopword For NLP
stopwords = requests.get("https://raw.githubusercontent.com/masdevid/ID-Stopwords/master/id.stopwords.02.01.2016.txt").text.split("\n")
tambahan = ['url', 'number', 'email', 'usd']
for tambah in tambahan: stopwords.append(tambah)

## Class Scraper Tempo.co
class tempoScrapper():
    def __init__(self):
        self

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


        subCategory = iSoup.select('.breadcrumbs > li')[1].text
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
            iData[i]['cleanContent'] = preprocess_text(iData[i]['content'], 
                                                lowercase=True, fix_unicode=True,no_punct=True,no_numbers=True,
                                                no_urls=True, no_currency_symbols=True,no_phone_numbers=True,
                                                no_emails=True)
            clean_content = iData[i]['cleanContent'].split()

            [text_stopword.append(cc) for cc in clean_content if cc not in stopwords]
            case_folding = ' '.join(text_stopword)
            # stemming = stemmer.stem(case_folding)

            iData[i]['cleanContent'] = case_folding

        return iData

    ## fungsi ini digunakan untuk membersihkan/membuang konten yang kosong
    def cleanData(self, iData=None):
        iResult = []
        for data in iData:
            if data['content'] != '':
                iResult.append(data)

        return iResult

    ## fungsi ini digunakan untuk mengambil sumber data dari Tempo.co secara harian
    def tempoDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):

        iData = []
        if month <= 9:
            if day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/0{}/{}'''.format(year, month, day, category)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/{}/{}'''.format(year, month, day, category)
        else:
            if day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/0{}/{}'''.format(year, month, day, category)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/{}/{}'''.format(year, month, day, category)

        print(iUrl)
        iResponse = requests.get(iUrl).text
        iSoup = BeautifulSoup(iResponse, "html5lib")
        iContents = iSoup.select('.list.list-type-1 > ul > li')

        for i in range(len(iContents)):
            tempUrl = iContents[i].select_one('a')['href']
            iTitle = iContents[i].select_one('.title').text
            iDate = iUrl.split('/')[6] + '-' + iUrl.split('/')[5] + '-' + iUrl.split('/')[4]

            iJson = {
                'category': nameCategory,
                'title': iTitle,
                'description': '',
                'url': tempUrl,
                'content': '',
                'contentHTML': '',
                'img': '',
                'subCategory': '',
                'publishedAt': iDate,
                'source': 'tempo.co',
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

    ## fungsi ini digunakan untuk mengambil sumber data dari Tempo.co secara bulanan
    def tempoMonthly(self, category=None, nameCategory=None, year=None, month=None):

        iData = []
        for i in tqdm(range(10), desc='Get Data'):
            try:
                if month <= 9:
                    if i + 1 <= 9:
                        iUrl = '''https://www.tempo.co/indeks/{}/0{}/0{}/{}'''.format(year, month, i + 1, category)
                    else:
                        iUrl = '''https://www.tempo.co/indeks/{}/0{}/{}/{}'''.format(year, month, i + 1, category)
                else:
                    if i + 1 <= 9:
                        iUrl = '''https://www.tempo.co/indeks/{}/{}/0{}/{}'''.format(year, month, i + 1, category)
                    else:
                        iUrl = '''https://www.tempo.co/indeks/{}/{}/{}/{}'''.format(year, month, i + 1, category)

                print(iUrl)
                iResponse = requests.get(iUrl).text
                iSoup = BeautifulSoup(iResponse, "html5lib")
                contents = iSoup.select('.list.list-type-1 > ul > li')

                for i in range(len(contents)):
                    tempUrl = contents[i].select_one('a')['href']
                    iTitle = contents[i].select_one('.title').text
                    iDate = iUrl.split('/')[6] + '-' + iUrl.split('/')[5] + '-' + iUrl.split('/')[4]

                    iJson = {
                        'category': nameCategory,
                        'title': iTitle,
                        'description': '',
                        'url': tempUrl,
                        'content': '',
                        'contentHTML': '',
                        'img': '',
                        'subCategory': '',
                        'publishedAt': iDate,
                        'source': 'tempo.co',
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
            except:
                pass

        return iData

    ## tempo daily with model MNB
    def tempoDailyModel(self, category=None, year=None, month=None, day=None):

        iData = []
        if month <= 9:
            if day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/0{}/{}'''.format(year, month, day, category)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/0{}/{}/{}'''.format(year, month, day, category)
        else:
            if day <= 9:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/0{}/{}'''.format(year, month, day, category)
            else:
                iUrl = '''https://www.tempo.co/indeks/{}/{}/{}/{}'''.format(year, month, day, category)

        print(iUrl)
        iResponse = requests.get(iUrl).text
        iSoup = BeautifulSoup(iResponse, "html5lib")
        iContents = iSoup.select('.list.list-type-1 > ul > li')

        for i in range(len(iContents)):
            tempUrl = iContents[i].select_one('a')['href']
            iTitle = iContents[i].select_one('.title').text
            iDate = iUrl.split('/')[6] + '-' + iUrl.split('/')[5] + '-' + iUrl.split('/')[4]

            iJson = {
                'category': '',
                'title': iTitle,
                'description': '',
                'url': tempUrl,
                'content': '',
                'contentHTML': '',
                'img': '',
                'subCategory': '',
                'publishedAt': iDate,
                'source': 'tempo.co',
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

    ## menentukan kategori artikel berita dengan model klasifikasi
    def getCategory(self, iData=None):

        model = joblib.load('modelMNB')

        for data in iData:
            clean = preprocess_text(data['cleanContent'], lowercase=True)
            result = model.predict([clean])
            result = result[0]

            data['category'] = result

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara harian
    def iDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):
        iData = self.tempoDaily(category, nameCategory, year, month, day)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara bulanan
    def iMonthly(self, category=None, nameCategory=None, year=None, month=None):
        iData = self.tempoMonthly(category, nameCategory, year, month)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData

    def iDailyModel(self, category=None, year=None, month=None, day=None):
        iData = self.tempoDailyModel(category, year, month, day)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)
        iData = self.getCategory(iData)

        return iData