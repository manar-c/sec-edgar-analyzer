#Get data from SEC
#Created by Manar El-Chammas
#Created on 7/22/2023

'''Overview:  map CIK to ticker and vice versa.  Download different reports related to target company.  Save and extrat relevant data'''

#This is based on: https://www.sec.gov/os/accessing-edgar-data
global_SEC_ticker_JSON_url = 'https://www.sec.gov/files/company_tickers.json'
global_SEC_ticker_exchange_JSON_url = 'https://www.sec.gov/files/company_tickers_exchange.json'

#JSON format for SEC
global_summary_JSON_url = 'https://data.sec.gov/submissions/CIK#.json' # The # is the number to be replaced
global_financial_companyfacts_JSON_url = 'https://data.sec.gov/api/xbrl/companyfacts/CIK#.json'

from urllib.request import Request, urlopen
import json
import os
from datetime import datetime
import sec_company_analysis
import zipfile
import pandas as pd


class SEC_EDGAR_WRAPPER:

    def __init__(self, hdr_info_agent, getLatest = True, baseDir = './'):
        self.baseDir = baseDir + '/'
        self.str_ticker = 'ticker'
        self.str_title = 'title'
        self.str_cik = 'cik_str'
        self.str_exchange = 'exchange'

        self.hdr_info_agent = hdr_info_agent #No error checks at this point in time
        self.url_hdr = {'User-Agent': self.hdr_info_agent}

        self.fname_ticker_json = 'ticker_cik_mapping.json'
        self.fname_ticker_exchange_json = 'ticker_exchange_cik_mapping.json'

        self.date_str = datetime.today().strftime('%Y-%m-%d')

        self.cik_length = 10 #Number of digits needed to access data

        self.str_cik_hash = '#' #This is used for search and replace

        self.tickerSummaryDir = 'StockSummaryJSON' #Store all the stock summary JSONs (With date)
        self.str_summary_filing = 'filings'
        self.str_financial_filing = 'finangialDetails'

        self.str_financial_file = '_financial_'

        if getLatest:
            if not os.path.exists(self.baseDir):
                os.mkdir(self.baseDir)
            self.loadCIK_JSON()
            self.analyzeCIK_JSON()
        else:
            self.load_OLD_JSON()

        #Below are parameters for stock summary json, filing information
        self.str_summary_filing_form = 'form'
        self.str_summary_report_date = 'reportDate'





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

    def saveJSONfile(self, dict, fname):
        # Save data as JSON file
        json_object = json.dumps(dict, indent=4)
        with open(self.baseDir + fname, 'w') as outfile:
            outfile.write(json_object)
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

    def formatCIK(self, cik):
        ''' SEC requires 10 digit number, so this zero-pads the CIK number'''
        return str(cik).zfill(self.cik_length)

    def getFilingSummary(self, ticker):
        print('Loading filing summary for {}'.format(ticker))
        if ticker not in self.ticker_cik_mapping.keys():
            print('ERROR: Ticker {} does not exist in cik mapping.  Exiting ...'.format(ticker))

        cik = self.ticker_cik_mapping[ticker]['cik']
        cik_str = self.formatCIK(cik)
        print('CIK for ticker {} = {}.  Formatting into'.format(ticker, cik_str))

        url = global_summary_JSON_url.replace(self.str_cik_hash, cik_str)
        print('URL to fetch is {}'.format(url))

        req = Request(url)
        for k in self.url_hdr.keys():
            req.add_header(k, self.url_hdr[k])

        try:
            response = urlopen(req)
        except:
            print('ERROR: Reading URL ({}) did not succeed. Exiting ...'.format(url))
            exit()
        filing_summary = json.loads(response.read())
        #print(filing_summary)

        #Make directory fo summaryJSON (with date)
        if not os.path.exists(self.baseDir + self.tickerSummaryDir):
            os.mkdir(self.baseDir + self.tickerSummaryDir)

        fname = self.summaryJSON_filename(ticker, self.date_str)#ticker + '_' +  self.date_str + '.json'
        self.saveJSONfile(filing_summary, self.tickerSummaryDir+'/'+fname)
        #print(filing_summary.keys())

    def loadJSONfile(self, fname):

        try:
            with open(self.baseDir + fname, 'r') as f:
                json_object = json.load(f)
                json_data = json_object
                return json_data
        except:
            print('ERROR: File open unsuccessful. Exiting...')
            exit()

    def summaryJSON_filename(self, ticker, date_str):
        return ticker + '_' + date_str + '.json'

    def financialJSON_filename(self, ticker, date_str):
        return ticker + self.str_financial_file + date_str + '.json'

    #Not sure of the utility of this function
    def getLatestFiling(self, ticker, date=-1):
        #Can overload the date if you want a specific date
        if date == -1:
            date_str = self.date_str
        else:
            date_str = date #Should have an error check that it is the correct format
        fname = self.summaryJSON_filename(ticker, date_str)
        json_data = self.loadJSONfile(self.tickerSummaryDir + '/' + fname)

        #Iterate over keys
        print(json_data[self.str_summary_filing]['recent'].keys())


    def getFinancialDetails(self, ticker, getNew=True):
        '''Downloads company facts from data.sec.gov'
        ticker: Company ticker symbol
        getNew: if true, download from the SEC.  If false, use the latest version that is saved.'''

        print('Loading financial details for {}'.format(ticker))
        if ticker not in self.ticker_cik_mapping.keys():
            print('ERROR: Ticker {} does not exist in cik mapping.  Exiting ...'.format(ticker))
            exit()

        cik = self.ticker_cik_mapping[ticker]['cik']
        cik_str = self.formatCIK(cik)
        print('CIK for ticker {} = {}.  Formatting into'.format(ticker, cik_str))

        if getNew:

            url = global_financial_companyfacts_JSON_url.replace(self.str_cik_hash, cik_str)
            print('URL to fetch is {}'.format(url))

            req = Request(url)
            for k in self.url_hdr.keys():
                req.add_header(k, self.url_hdr[k])
            try:
                response = urlopen(req)
            except:
                print('ERROR: Reading URL ({}) did not succeed. Exiting ...'.format(url))
                exit()
        
            financialDetails = json.loads(response.read())
            # print(filing_summary)

            # Make directory to store file
            if not os.path.exists(self.baseDir + self.tickerSummaryDir):
                os.mkdir(self.baseDir + self.tickerSummaryDir)

            fname = self.tickerSummaryDir + '/' + self.financialJSON_filename(ticker, self.date_str)  # ticker + '_' +  self.date_str + '.json'
            self.saveJSONfile(financialDetails, fname)
        else:
            print('Using already downloaded file...')
            fList = os.listdir(self.baseDir + '/' + self.tickerSummaryDir)
            fTickerList = []
            for f in fList:
                if ticker in f and self.str_financial_file in f:
                    fTickerList.append(f)

            fname = self.tickerSummaryDir + '/' + sorted(fTickerList, reverse=True)[0] #Get most recent

        # print(filing_summary.keys())
        print('Saved data in {}'.format(fname))
        
        return fname #For access later

    def loadFinancialDetails(self, fname):
        ''' Load financial JSON and parse it into something meaningful '''
        data = self.loadJSONfile(fname)

        return data

    def getFinancialDataSet(self, getNew = True):
        ''' Downloads a zip file that has entries on balance sheets, etc.  Can't find a clear XBRL mapping'''

        #Should automate this part, in terms of finding the latest file.  For now, it is ok
        ftarget = '2023q2.zip'
        if getNew:

            z1 = Request('https://www.sec.gov/files/dera/data/financial-statement-data-sets/{}'.format(ftarget))
            #z = urlopen(z1)

            if not os.path.exists('download'):
                os.makedirs('download')

            with urlopen(z1) as response, open('download/{}'.format(ftarget), 'wb') as out_file:
                data = response.read()  # a `bytes` object
                out_file.write(data)

        with zipfile.ZipFile('download/{}'.format(ftarget), 'r') as zObject:
            # Extracting all the members of the zip
            # into a specific location.
            zObject.extractall(path='download/{}'.format(ftarget.split('.')[0]))

        return 'download/{}'.format(ftarget.split('.')[0])

if __name__ == '__main__':
    #https://www.sec.gov/Archives/edgar/data/320193/000032019320000096/
    #the above contains all the filings.  the first number is the cik.  The second is the acct number in the json (without the -)
    ''' For example, can download the xml here: https://www.sec.gov/Archives/edgar/data/320193/000032019320000096/aapl-20200926_htm.xml 
    This contains the company fact and the value, as in the json.  But, I can't find which sheet it is in.  Does that mean it isn't in any sheet?
    For example, FinanceLeaseLiabilityPaymentsDueNextTwelveMonths isn't in the ata downloads, but is in the JSON.  I also find it directly in the filing
    https://www.sec.gov/ix?doc=/Archives/edgar/data/320193/000032019320000096/aapl-20200926.htm (I search for the term).  
    It seems that the JSON has lots of extra info that don't directly appear in the statements, but provide additional information
    For eample, there are multiple parts for the lease, but they aren't all listed in the balance sheet'''
    #Income statement doesn't have c or d
    db = SEC_EDGAR_WRAPPER(hdr_info_agent = 'Levant Labs LLC.  manar@levant-labs.com', getLatest=False, baseDir='json_data')
    #Data is now in: db.ticker_exchange_cik_mapping and db.ticker_cik_mapping
    print(db.ticker_cik_mapping['AAPL'])
    targetCIK = db.ticker_cik_mapping['AAPL']

    db.getFilingSummary('AAPL')
    db.getLatestFiling('AAPL')
    f_apple = db.getFinancialDetails('AAPL', getNew = False)
    data = db.loadFinancialDetails(f_apple)

    #Now, set up company analysis
    sAAPL = sec_company_analysis.SEC_COMPANY_FINANCIALS()
    status = sAAPL.loadFinancials('AAPL', data)
    print('Status of financial data loading = {}'.format(status))
    #exit()
    if status:
        cf = sAAPL.getCompanyFacts(form='10-K', fy=2019, fp='FY')
        print(cf)
    else:
        print('Exiting since load financial failed...')
        exit()

    exit()

    #path = db.getFinancialDataSet(getNew = False)
    print('Detailed data from SEC saved here: {}'.format(path))
    print('Will start to analyze this data ...')

    listF = os.listdir(path)
    for f in listF:
        print(f)

    #The readme.htm file in the unzipped location has quite a bit of info
    #Key files to look at: 1) sub.txt --> contains CIK and ADSH.  Contains other general information, not sure the rest is useful
    #tag.txt contains all the taxonomy tags, and whether they are custom or not.  Not sure I are about that right now
    #pre.txt contains adsh, tag, and stmt.  Can have two stms.  ALPHANUMERIC (BS = Balance Sheet, IS = Income Statement, CF = Cash Flow, EQ = Equity, CI = Comprehensive Income, UN = Unclassifiable Statement).
    #Not sure I need num.txt?

    #df_sub = pd.read_csv(path + '/' + 'sub.txt', sep='\t')
    df_sub = pd.read_csv('download/2009q1_2023q2_sub.csv')#, sep='\t')
    print(targetCIK)
    print(df_sub.columns)
    print(df_sub.cik)
    red = df_sub[df_sub.cik == targetCIK['cik']]
    print(red)

    #Load pre .txt

    #df_pre = pd.read_csv(path + '/' + 'pre.txt', sep='\t')
    df_pre = pd.read_csv('download/' + '2009q1_2023q2_pre.csv')#, sep='\t')
    print(df_pre.columns)

    df_tag = pd.read_csv('download/' + '2009q1_2023q2_tag.csv')  # , sep='\t')


    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    temp = []
    for i in red.adsh:
        print(i)
        temp.append(df_pre[(df_pre.adsh == i)])# & (df_pre.abstract == 0)] #Abstract is for numeric facts
        #print('{}, {}'.format(temp.tag, temp.stmt))
        #print(temp)

    #print(temp)
    #Compare to temp.tag
    print('AAPL company facts = {}'.format(cf))

    for c in cf:

        isFound = False
        #x = df_pre.tag.tolist()
        #if c in x:
        #    isFound = True
        for t in temp:
            tL = t.tag.tolist()
            sL = t.stmt.tolist()

            if c in tL:
                isFound = True
                ind = tL.index(c)
                crdr = set(df_tag[df_tag.tag == c].crdr.tolist())
                #print('FOUND!!')
                print('FOUND: Looking for {}.  STMT = {}.  CRDR = {}'.format(c, sL[ind], crdr))

        if isFound:
            #print('FOUND: Looking for {}'.format(c))
            trash = 1
        else:
            print('** ERROR: Looking for {}'.format(c))

    #Now, cycle through each cf, and see if it exists in pre with the cik



    #EXample.  JSON that contains history of filings for accounts payable
    #https://data.sec.gov/api/xbrl/companyconcept/CIK0000320193/us-gaap/AccountsPayableCurrent.json
    #https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json (additional info)
    #The above contains lots of data, this is a lot of financial data!
    #https://data.sec.gov/api/xbrl/frames/us-gaap/AccountsPayableCurrent/USD/CY2019Q1I.json (gives information for different companies)