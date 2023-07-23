#Get data from SEC
#Created by Manar El-Chammas
#Created on 7/22/2023

'''Overview:  map CIK to ticker and vice versa.  Download different reports related to target company.  Save and extrat relevant data'''

#This is based on: https://www.sec.gov/os/accessing-edgar-data
global_SEC_ticker_JSON_url = 'https://www.sec.gov/files/company_tickers.json'
global_SEC_ticker_exchange_JSON_url = 'https://www.sec.gov/files/company_tickers_exchange.json'

from urllib.request import urlopen
import json
import os
from datetime import datetime


class SEC_EDGAR_WRAPPER:

    def __init__(self, getLatest = True, baseDir = './'):
        self.baseDir = baseDir + '/'
        self.str_ticker = 'ticker'
        self.str_title = 'title'
        self.str_cik = 'cik_str'
        self.str_exchange = 'exchange'

        self.fname_ticker_json = 'ticker_cik_mapping.json'
        self.fname_ticker_exchange_json = 'ticker_exchange_cik_mapping.json'

        self.date_str = datetime.today().strftime('%Y-%m-%d')

        if getLatest:
            if not os.path.exists(self.baseDir):
                os.mkdir(self.baseDir)
            self.loadCIK_JSON()
            self.analyzeCIK_JSON()
        else:
            self.load_OLD_JSON()





    def loadCIK_JSON(self):
        '''Load the JSON files for latet access'''

        url = global_SEC_ticker_JSON_url
        try:
            response = urlopen(url)
        except:
            print('ERROR: Reading URL ({}) did not succeed. Exiting ...'.format(url))
            exit()
        self.CIK_TICKER_JSON = json.loads(response.read())
        #Save data with date
        json_object = json.dumps(self.CIK_TICKER_JSON, indent=4)
        with open(self.baseDir + 'SEC_DATA_CIK_TICKER_{}.json'.format(self.date_str), 'w') as outfile:
            outfile.write(json_object)

        url = global_SEC_ticker_exchange_JSON_url
        try:
            response = urlopen(url)
        except:
            print('ERROR: Reading URL ({}) did not succeed. Exiting ...'.format(url))
            exit()
        self.CIK_TICKER_EXCHANGE_JSON = json.loads(response.read())

        json_object = json.dumps(self.CIK_TICKER_EXCHANGE_JSON, indent=4)
        with open(self.baseDir + 'SEC_DATA_CIK_TICKER_EXCHANGE_{}.json'.format(self.date_str), 'w') as outfile:
            outfile.write(json_object)


        #print(self.CIK_JSON)

    def analyzeCIK_JSON(self):
        '''Analyze and reformat JSON data for easier access'''

        #Iterate through JSON and set ticker as the key

        self.ticker_cik_mapping = {}

        for k in self.CIK_TICKER_JSON.keys():
            temp = self.CIK_TICKER_JSON[k]
            ticker = temp[self.str_ticker]
            cik = temp[self.str_cik]
            title = temp[self.str_title]
            #Make sure ticker doesn't already exist
            if ticker in self.ticker_cik_mapping.keys():
                print('ERROR: Ticker {} already exists in mapping.  Exiting...'.format(ticker))
                exit()
            self.ticker_cik_mapping[ticker] = {'cik': cik, 'title': title}

        #Save data as JSON file
        json_object = json.dumps(self.ticker_cik_mapping, indent=4)
        with open(self.baseDir + self.fname_ticker_json, 'w') as outfile:
            outfile.write(json_object)

        #Second JSON file
        self.ticker_exchange_cik_mapping = {}
        print(self.CIK_TICKER_EXCHANGE_JSON)
        #This is formatted differently.  Field, and data parameters
        fields = self.CIK_TICKER_EXCHANGE_JSON['fields']
        data = self.CIK_TICKER_EXCHANGE_JSON['data']
        for i in range(len(fields)):
            if fields[i] == 'cik':
                index_cik = i
            elif fields[i] == 'name':
                index_name = i
            elif fields[i] == 'ticker':
                index_ticker = i
            elif fields[i] == 'exchange':
                index_exchange = i

        for temp in data:
            #temp = self.CIK_TICKER_EXCHANGE_JSON[k]
            ticker = temp[index_ticker]
            cik = temp[index_cik]
            title = temp[index_name]
            exchange = temp[index_exchange]
            #Make sure ticker doesn't already exist
            if ticker in self.ticker_exchange_cik_mapping.keys():
                print('ERROR: Ticker {} already exists in mapping.  Exiting...'.format(ticker))
                exit()
            self.ticker_exchange_cik_mapping[ticker] = {'cik': cik, 'title': title, 'exchange': exchange}

        #Save data as JSON file
        json_object = json.dumps(self.ticker_exchange_cik_mapping, indent=4)
        with open(self.baseDir + self.fname_ticker_exchange_json, 'w') as outfile:
            outfile.write(json_object)

    def load_OLD_JSON(self):
        #See if file exists and then load it
        try:
            with open(self.baseDir + self.fname_ticker_json, 'r') as f:
                json_object = json.load(f)
                self.ticker_cik_mapping = json_object
        except:
            print('ERROR: File open unsuccessful. Exiting...')
            exit()

        try:
            with open(self.baseDir + self.fname_ticker_exchange_json, 'r') as f:
                json_object = json.load(f)
                self.ticker_exchange_cik_mapping = json_object
        except:
            print('ERROR: File open unsuccessful. Exiting...')
            exit()



if __name__ == '__main__':
    db = SEC_EDGAR_WRAPPER(getLatest=True, baseDir='json_data')
    #Data is now in: db.ticker_exchange_cik_mapping and db.ticker_cik_mapping
