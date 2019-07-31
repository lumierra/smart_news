from textacy.preprocess import preprocess_text
# from Database.dbMongo import Database
from bs4 import BeautifulSoup
from tqdm import tqdm
import id_beritagar as indo
import datetime
import requests
import id_aldo
import sys

sys.path.insert(0, '/home/lumierra/smart_news/Database/')
import dbMongo

## Load NLP
nlp = id_aldo.load()
nlp_ner = indo.load()

## Load Database Mongo
DB = dbMongo.Database()

## Load Stopword For NLP
stopwords = requests.get("https://raw.githubusercontent.com/masdevid/ID-Stopwords/master/id.stopwords.02.01.2016.txt").text.split("\n")

## set datetime
now = datetime.datetime.now().date()

class liputanScraper():

    def __init__(self):
        self.year = now.year
        self.month = now.month
        self.day = now.day

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

        img = iSoup.select_one('.read-page--photo-gallery--item__picture > img')['data-src']
        contents = iSoup.select('.article-content-body__item-content > p')

        for i in range(len(contents)):
            if contents[i].text.strip() != '' and contents[i].text.strip()[:1] != '*' \
                    and contents[i].text.strip()[:8] != 'Reporter' and contents[i].text.strip()[:14] != 'Saksikan video'\
                    and contents[i].text.strip()[:1] != '(' and contents[i].text.strip()[:14] != 'Saksikan Video' \
                    and contents[i].text.strip()[:2] != ' (' and contents[i].text.strip()[:7] != 'Sumber:':
                iData.append(contents[i].text.strip() + '\n\n')

        ordinaryContent = ''.join(iData)
        ordinaryContent = preprocess_text(ordinaryContent, fix_unicode=True)
        ordinaryContent = self.nerText(ordinaryContent)
        htmlContent = ''.join(iData)
        htmlContent = self.nerText(htmlContent)
        htmlContent = htmlContent.split('\n\n')

        iJson = {
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
                iData[i]['img'] = iTemp['img']
                iData[i]['contentHTML'] = iTemp['contentHTML']
            except:
                pass

        return iData

    ## fungsi ini digunakan untuk membersihkan/membuang konten yang kosong
    def cleanData(self, iData=None):
        iResult = []
        for ad in iData:
            if ad['content'] != '':
                iResult.append(ad)

        return iResult

    ## fungsi ini digunakan untuk membersihkan konten di dalam artikel berita
    def cleanContent(self, iData=None):
        for i in tqdm(range(len(iData)), desc='Clean Content'):
            iStopword = []
            iData[i]['cleanContent'] = preprocess_text(iData[i]['content'], lowercase=True, fix_unicode=True,no_punct=True)
            cleanContent = iData[i]['cleanContent'].split()

            [iStopword.append(cc) for cc in cleanContent if cc not in stopwords]

            iData[i]['cleanContent'] = ' '.join(iStopword)

        return iData

    ## fungsi ini digunakan untuk mengambil data (crawler) artikel berita pada liputan6 secara perbulan
    def liputanMonthly(self, category=None, nameCategory=None, year=None, month=None):
        iData = []
        for i in tqdm(range(31), desc='Get Data monthly'):
            for page in range(2):
                if month <= 9:
                    if i + 1 <= 9:
                        iUrl = '''https://www.liputan6.com/{}/indeks/{}/0{}/0{}?page={}'''.format(category, year, month,
                                                                                                 i + 1, page + 1)
                    else:
                        iUrl = '''https://www.liputan6.com/{}/indeks/{}/0{}/{}?page={}'''.format(category, year, month,
                                                                                                i + 1, page + 1)
                else:
                    if i + 1 <= 9:
                        iUrl = '''https://www.liputan6.com/{}/indeks/{}/{}/0{}?page={}'''.format(category, year, month,
                                                                                                i + 1, page + 1)
                    else:
                        iUrl = '''https://www.liputan6.com/{}/indeks/{}/{}/{}?page={}'''.format(category, tahun, bulan,
                                                                                               i + 1, page + 1)

                print(iUrl)
                iResponse = requests.get(iUrl).text
                iSoup = BeautifulSoup(iResponse, "html5lib")
                contents = iSoup.select('.articles--rows--item__details')

                for y in range(len(contents)):
                    title = contents[y].select_one('.articles--rows--item__title').text

                    if title[:6] != 'VIDEO:' and title[:5] != 'FOTO:' and title[:6] != 'FOTO :' and title[:5] != 'Top 3' and title[:4] != 'Top3':
                        url = contents[y].select_one('.articles--rows--item__title > a')['href']
                        iCategory = iUrl.split('/')[3]
                        subCategory = contents[y].select_one('.articles--rows--item__category').text
                        iTitle = contents[y].select_one('.articles--rows--item__title').text
                        iDescription = contents[y].select_one('.articles--rows--item__summary').text
                        iDate = iUrl.split('/')[7].split('?')[0] + '-' + iUrl.split('/')[6] + '-' + iUrl.split('/')[5]

                        iJson = {
                            "category": nameCategory,
                            "title": iTitle,
                            "description": iDescription,
                            "url": url,
                            "content": '',
                            "contentHTML": '',
                            "img": '',
                            "subCategory": subCategory,
                            "publishedAt": iDate,
                            "source": 'liputan6.com',
                            "cleanContent": '',
                            "nerContent": '',
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

    ## fungsi ini digunakan untuk mengambil data (crawler) artikel berita pada liputan6 secara perhari
    def liputanDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):
        iData = []
        for i in range(2):
            if month <= 9:
                if day <= 9:
                    iUrl = '''https://www.liputan6.com/{}/indeks/{}/0{}/0{}?page={}'''.format(category, year, month, day, i + 1)
                else:
                    iUrl = '''https://www.liputan6.com/{}/indeks/{}/0{}/{}?page={}'''.format(category, year, month, day, i + 1)
            else:
                if day <= 9:
                    iUrl = '''https://www.liputan6.com/{}/indeks/{}/{}/0{}?page={}'''.format(category, year, month, day, i + 1)
                else:
                    iUrl = '''https://www.liputan6.com/{}/indeks/{}/{}/{}?page={}'''.format(category, year, month, day, i + 1)

            print(iUrl)
            iResponse = requests.get(iUrl).text
            iSoup = BeautifulSoup(iResponse, "html5lib")
            contents = iSoup.select('.articles--rows--item__details')

            for y in range(len(contents)):
                title = contents[y].select_one('.articles--rows--item__title').text

                if title[:6] != 'VIDEO:' and title[:5] != 'FOTO:' and title[:6] != 'FOTO :' and title[:5] != 'Top 3' and title[:4] != 'Top3':
                    url = contents[y].select_one('.articles--rows--item__title > a')['href']
                    category = iUrl.split('/')[3]
                    subCategory = contents[y].select_one('.articles--rows--item__category').text
                    title = contents[y].select_one('.articles--rows--item__title').text
                    description = contents[y].select_one('.articles--rows--item__summary').text
                    date = iUrl.split('/')[7].split('?')[0] + '-' + iUrl.split('/')[6] + '-' + iUrl.split('/')[5]

                    iJson = {
                        "category": nameCategory,
                        "title": title,
                        "description": description,
                        "url": url,
                        "content": '',
                        "contentHTML": '',
                        "img": '',
                        "subCategory": subCategory,
                        "publishedAt": date,
                        "source": 'liputan6.com',
                        "cleanContent": '',
                        "nerContent": '',
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

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara perhari
    def iDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):
        iData = self.liputanDaily(category, nameCategory, year, month, day)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara perhari
    def iMonthly(self, category=None, nameCategory=None, year=None, month=None):
        iData = self.liputanMonthly(category, nameCategory, year, month)
        iData = self.getContent2((iData))
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData