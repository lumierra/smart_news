import pymongo
import requests
from tqdm import tqdm
from textacy.preprocess import  preprocess_text
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory

# create stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()

stopwords = requests.get("https://raw.githubusercontent.com/masdevid/ID-Stopwords/master/id.stopwords.02.01.2016.txt").text.split("\n")

tambahan = ['url', 'number', 'email', 'usd']
for tambah in tambahan: stopwords.append(tambah)

class Content():
    def __init__(self):
        self

    def insertData(self, attr=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.dataset

        try:
            collection.insert_many(attr)
            print('Insert Data into MongoDB Successfully')
        except:
            print('Insert Data into Mongod Failed')

    def deleteData(self, category=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.dataset

        iQuery = collection.remove({
            'category' : '{}'.format(category)
        })

        return iQuery
    
    def getData(self, category=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.backup_dataset
        
        data = []
        iQuery = collection.find({
            'category' : '{}'.format(category)
        })

        for query in iQuery: data.append(query)

        return data

    def getiData(self, url=None):
        client = pymongo.MongoClient('mongodb://localhost:27017')
        database = client.iStorage
        collection = database.iData

        data = []
        iQuery = collection.find(
            {'url' : '{}'.format(url)},
            {
                'title'   : 1,
                'url'     : 1,
                'content' : 1
            }
        )
        
        for query in iQuery: data.append(query)
            
        return data

    def stepOne(self, content=None):
        result = preprocess_text(content, fix_unicode=True, lowercase=True, no_urls=True,
                            no_emails=True, no_phone_numbers=True, no_numbers=True,
                            no_currency_symbols=True, no_punct=True)

        return result

    def stepTwo(self, content=None):
        text_stopword = []
        words = content.split()
        for word in words:
            if word not in stopwords: text_stopword.append(word)
                
        result = ' '.join(text_stopword)

        return result

    def stepThree(self, content=None):
        result = stemmer.stem(content)

        return result


    def cleanContent(self):
        # iData = []
        # categories = ['news', 'bisnis', 'sports', 'entertainment', 'tekno', 'otomotif', 'health']

        dataset = self.getData('otomotif')
        for i in tqdm(range(len(dataset))):
            temp = self.getiData(dataset[i]['url'])
            clean = temp[0]['content']
            clean = self.stepOne(clean)
            clean = self.stepTwo(clean)
            clean = self.stepThree(clean)
            
            dataset[i]['cleanContent'] = clean

        return dataset

    def execute(self, data=None):
        attr = self.cleanContent()
        self.insertData(attr)

run = Content()
run.execute()
            
           


