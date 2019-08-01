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

class kompasScraper():

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
        n = 0
        temp = 0
        for ent in doc.ents:
            if ent.end <= 1:
                n = ent.end_char
            elif (ent.end > 6 and ent.end <= 8):
                n = temp
            elif ent.end > 9:
                n = temp
            else:
                temp = n
                n = ent.end_char + 1

        iResult = text[n:].strip()

        return iResult

    ## fungsi ini digunakan untuk mendapatkan konten artikel berita
    def getContent(self, url=None):
        iResponse = requests.get(url).text
        iSoup = BeautifulSoup(iResponse, "html5lib")
        contents = iSoup.select_one('.photo > img')
        contents2 = iSoup.select('.read__content > p')
        img = contents['src']

        iData = []
        for i in range(len(contents2)):
            if contents2[i].text != '':
                if (contents2[i].text[:9] != 'Baca juga' and contents2[i].text[:5] != 'Baca:') \
                        and (contents2[i].text[:15] != 'We are thrilled') and (contents2[i].text[:6] != 'Flinke') \
                        and (contents2[i].text[:18] != 'Baca selengkapnya:') and (contents2[i].text[:25]) != 'Baca berita selengkapnya:' \
                        and (contents2[i].text[:7]) != 'Sumber:':
                    iData.append(contents2[i].text  + '\n\n')

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
        for i in tqdm(range(len(iData))):
            try:
                iTemp = self.getContent(iData[i]['url'])
                iData[i]['content'] = iTemp['content']
                iData[i]['contentHTML'] = iTemp['contentHTML']
                iData[i]['img'] = iTemp['img']
                iData[i]['description'] = iData[i]['title'] + ' ' + iData[i]['content'][:255] + '....'
            except:
                pass

        return iData

    ## fungsi ini digunakan untuk membersihkan konten di dalam artikel berita
    def cleanContent(self, iData=None):
        for i in tqdm(range(len(iData)), desc='Clean Content'):
            iStopword = []
            iData[i]['cleanContent'] = preprocess_text(iData[i]['content'], lowercase=True, fix_unicode=True,no_punct=True)
            cleanContent = iData[i]['cleanContent'].split()

            [iStopword.append(cc) for cc in cleanContent if cc not in stopwords]

            iData[i]['cleanContent'] = ' '.join(iStopword)

        return iData

    ## fungsi ini digunakan untuk membersihkan/membuang konten yang kosong
    def cleanData(self, iData=None):
        iResult = []
        for ad in iData:
            if ad['content'] != '':
                iResult.append(ad)

        return iResult

    ## fungsi ini digunakan untuk mengambil data (crawler) artikel berita pada liputan6 secara perbulan
    def kompasMonthly(self, category=None, nameCategory=None, tahun=None, bulan=None):
        all_data = []
        for tanggal in tqdm(range(31), desc='Get Data Monthly'):
            try:
                url = '''https://{}.kompas.com/search/{}-{}-{}'''.format(category, tahun, bulan, tanggal + 1)
                data = requests.get(url)
                html = data.text
                soup = BeautifulSoup(html, "html5lib")
                count_page = soup.select('.paging__wrap.clearfix > .paging__item')

                if count_page == []:
                    url_lokal = '''https://{}.kompas.com/search/{}-{}-{}'''.format(global_category, tahun, bulan,
                                                                                   tanggal + 1)
                    data = requests.get(url_lokal)
                    html = data.text
                    soup = BeautifulSoup(html, "html5lib")

                    contents = soup.select('.article__list.clearfix')
                    print(url_lokal)

                    for content in contents:
                        try:
                            temp_category = content.select_one('.article__subtitle').text.strip()
                            temp_url = content.select_one('.article__link')['href']
                            temp_title = content.select_one('.article__link').text.strip()
                            temp_date = content.select_one('.article__date').text.replace(',', '').split()[0]
                            temp_date = datetime.datetime.strptime(temp_date, "%d/%m/%Y").strftime("%d-%m-%Y")

                            data_json = {
                                "category": nameCategory,
                                "title": temp_title,
                                "description": '',
                                "url": temp_url,
                                "content": '',
                                "content_html": '',
                                "img": '',
                                "sub_category": temp_category,
                                "publishedAt": temp_date,
                                "source": 'kompas.com',
                                "clean_content": '',
                                "ner_content": '',
                                'count_ner': {
                                    'person': 0,
                                    'org': 0,
                                    'gpe': 0,
                                    'event': 0,
                                    'merk': 0,
                                    'product': 0
                                }

                            }

                            all_data.append(data_json)

                        except:
                            pass
                else:
                    total_page = int(count_page[len(count_page) - 1].select('.paging__link')[0]['data-ci-pagination-page'])

                    for y in range(total_page):
                        try:
                            url_lokal = '''https://{}.kompas.com/search/{}-{}-{}/{}'''.format(global_category, tahun, bulan,
                                                                                              tanggal + 1, y + 1)
                            data = requests.get(url_lokal)
                            html = data.text
                            soup = BeautifulSoup(html, "html5lib")

                            contents = soup.select('.article__list.clearfix')
                            print(url_lokal)

                            for content in contents:
                                try:
                                    temp_category = content.select_one('.article__subtitle').text.strip()
                                    temp_url = content.select_one('.article__link')['href']
                                    temp_title = content.select_one('.article__link').text.strip()
                                    temp_date = content.select_one('.article__date').text.replace(',', '').split()[0]
                                    temp_date = datetime.datetime.strptime(temp_date, "%d/%m/%Y").strftime("%d-%m-%Y")

                                    data_json = {
                                        "category": name_category,
                                        "title": temp_title,
                                        "description": '',
                                        "url": temp_url,
                                        "content": '',
                                        "content_html": '',
                                        "img": '',
                                        "sub_category": temp_category,
                                        "publishedAt": temp_date,
                                        "source": 'kompas.com',
                                        "clean_content": '',
                                        "ner_content": '',
                                        'count_ner': {
                                            'person': 0,
                                            'org': 0,
                                            'gpe': 0,
                                            'event': 0,
                                            'merk': 0,
                                            'product': 0
                                        }
                                    }

                                    all_data.append(data_json)

                                except:
                                    pass
                        except:
                            pass
            except:
                pass

        return all_data

    ## fungsi ini digunakan untuk mengambil data (crawler) artikel berita pada liputan6 secara perhari
    def kompasDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):
        iData = []
        iUrl = '''https://{}.kompas.com/search/{}-{}-{}'''.format(category, year, month, day)
        iResponse = requests.get(iUrl).text
        iSoup = BeautifulSoup(iResponse, "html5lib")
        countPage = iSoup.select('.paging__wrap.clearfix > .paging__item')

        if countPage == []:
            url = '''https://{}.kompas.com/search/{}-{}-{}'''.format(category, year, month, day)
            iResponse = requests.get(url).text
            iSoup = BeautifulSoup(iResponse, "html5lib")
            contents = iSoup.select('.article__list.clearfix')
            print(url)

            for content in contents:
                try:
                    iCategory = content.select_one('.article__subtitle').text.strip()
                    realUrl = content.select_one('.article__link')['href']
                    iTitle = content.select_one('.article__link').text.strip()
                    iDate = content.select_one('.article__date').text.replace(',', '').split()[0]
                    iDate = datetime.datetime.strptime(iDate, "%d/%m/%Y").strftime("%d-%m-%Y")

                    iJson = {
                        "category": nameCategory,
                        "title": iTitle,
                        "description": '',
                        "url": realUrl,
                        "content": '',
                        "contentHTML": '',
                        "img": '',
                        "subCategory": iCategory,
                        "publishedAt": iDate,
                        "source": 'kompas.com',
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

                except:
                    pass
        else:
            totalPage = int(countPage[len(countPage) - 1].select('.paging__link')[0]['data-ci-pagination-page'])

            for y in range(totalPage):
                try:
                    url = '''https://{}.kompas.com/search/{}-{}-{}/{}'''.format(category, year, month, day, y + 1)
                    iResponse = requests.get(url).text
                    iSoup = BeautifulSoup(iResponse, "html5lib")
                    contents = iSoup.select('.article__list.clearfix')
                    print(url)

                    for content in contents:
                        try:
                            iCategory = content.select_one('.article__subtitle').text.strip()
                            realUrl = content.select_one('.article__link')['href']
                            iTitle = content.select_one('.article__link').text.strip()
                            iDate = content.select_one('.article__date').text.replace(',', '').split()[0]
                            iDate = datetime.datetime.strptime(iDate, "%d/%m/%Y").strftime("%d-%m-%Y")

                            iJson = {
                                "category": nameCategory,
                                "title": iTitle,
                                "description": '',
                                "url": realUrl,
                                "content": '',
                                "contentHTML": '',
                                "img": '',
                                "subCategory": iCategory,
                                "publishedAt": iDate,
                                "source": 'kompas.com',
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

                        except:
                            pass
                except:
                    pass

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara perhari
    def iDaily(self, category=None, nameCategory=None, year=None, month=None, day=None):
        iData = self.kompasDaily(category, nameCategory, year, month, day)
        iData = self.getContent2(iData)
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData

    ## fungsi ini digunakan untuk menjalankan semua fungsi yang dibutuhkan untuk mengambil data artikel berita secara perbulan
    def iMonthly(self, category=None, nameCategory=None, year=None, month=None):
        iData = self.kompasMonthly(category, nameCategory, year, month)
        iData = self.getContent2(iData)
        iData = self.cleanData(iData)
        iData = self.cleanContent(iData)

        return iData