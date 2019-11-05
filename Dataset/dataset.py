from tqdm import tqdm
import sys

sys.path.insert(0, '/home/lumierra/smart_news/Database/')
import dbMongo

## Load Database Mongo
DB = dbMongo.Database()

class Dataset():
    def __init__(self):
        self.database = 'iStorage'
        self.collection = 'iData'
    
    def makeDataset(self, category=None):
        data = DB.getDataset(self.database, self.collection, '{}'.format(category))
        benar = 0
        salah = 0
        for i in tqdm(range(len(data))):
            print(data[i]['title'])
            print('Kategori : ' + data[i]['category'])
            print(get_categoryMNB(data[i]['title']))
            if data[i]['category'] == get_categoryMNB(data[i]['title']): 
                benar+=1
                print('Benar')
                insertData(data[i])
            else: 
                salah+=1
                print('Salah')
            print('======================================\n')

        print('=============================')
        print('Benar : {}'.format(benar))
        print('Salah : {}'.format(salah))