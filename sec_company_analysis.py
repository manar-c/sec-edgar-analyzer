#Loads saved JSON data to analyze

import json
#import rich.table as rTable
from rich.table import Table as rTable
from rich.console import Console as rConsole
from rich import box
import pandas as pd
import os
class SEC_COMPANY_FINANCIALS:
    def __init__(self, analysisFolder = 'companyfiles'):
        self.tickerExists = False
        self.analysisFolder = analysisFolder
        #Create folder if not exist
        if not os.path.exists(self.analysisFolder):
            os.mkdir(self.analysisFolder)


        #Keys below are from JSON file.  If that changes, this needs to change as well
        self.key_CIK =  'cik'
        self.key_entity = 'entityName'
        self.key_facts = 'facts'
        self.key_facts_dei = 'dei' #Document and Entity Information
        self.key_facts_gaap = 'us-gaap'

        self.cik = -1
        self.companyName = ''

        #Unsure if the dei is the same across all.  Will need to test it out.  For now, no need to make it general
        self.key_dei_shares = 'EntityCommonStockSharesOutstanding'
        self.key_dei_float = 'EntityPublicFloat'

        self.isProcessed = False #Once data is processed, set to True

        self.form_types = ['10-K', '10-Q']
        self.form_10K = '10-K'
        self.form_10Q = '10-Q'

    def loadFinancials(self, ticker, data):
        if len(ticker) > 0:
            self.tickerExists = True
            self.ticker = ticker
            self.financialData = data
            #print(self.financialData.keys())


        else:
            return False

        #Time to do some parsing
        self.cik = data[self.key_CIK]
        self.companyName = data[self.key_entity]
        data_details = data[self.key_facts]

        #print(data_details.keys())

        #print(data_details[self.key_facts_dei])


        #self.deiTable = {}
        ##x = rTable()
        #self.deiTable[self.key_dei_shares] = rTable()
        #self.deiTable[self.key_dei_float] = rTable()

        tempShares = data_details[self.key_facts_dei][self.key_dei_shares]
        self.dei_Shares = {}
        self.dei_Float = {}
        self.key_label = 'label'
        self.key_description = 'description'
        self.key_units = 'units'
        self.key_shares = 'shares'
        self.key_USD = 'USD'
        self.key_data_end = 'end'
        self.key_data_val = 'val'
        self.key_data_accn = 'accn'
        self.key_data_fy = 'fy'
        self.key_data_fp = 'fp'
        self.key_data_form= 'form'
        self.key_data_filed = 'filed'
        self.key_data_frame = 'frame'
        self.key_data = 'DATA'

        print('Number of entries in DEI_SHARES = {}. Expecting 3...'.format(len(tempShares.keys())))
        self.dei_Shares[self.key_label] = tempShares[self.key_label]
        self.dei_Shares[self.key_description] = tempShares[self.key_description]

        print('Number of entriess in DEI_SHARES_UNITS = {}. Expecting 1...'.format(len(tempShares[self.key_units].keys())))
        tempUnits = tempShares[self.key_units][self.key_shares]
        #print(json.dumps(tempUnits, indent=4))

        #df = pd.DataFrame()
        self.dei_Shares['DATA'] = pd.json_normalize(tempUnits)
        #This contains the entire JSON for a summary of the financial report
        #self._data = self.dei_Shares['DATA']
        #print(self.dei_Shares['DATA'])
        #return True

        #Now, load at float
        tempFloat = data_details[self.key_facts_dei][self.key_dei_float]
        print('Number of entries in DEI_FLOAT = {}. Expecting 3...'.format(len(tempFloat.keys())))
        self.dei_Float[self.key_label] = tempFloat[self.key_label]
        self.dei_Float[self.key_description] = tempFloat[self.key_description]

        print('Number of entries in DEI_FLOAT_UNITS = {}. Expecting 1...'.format(len(tempFloat[self.key_units].keys())))
        tempUnits = tempFloat[self.key_units][self.key_USD]
        # print(json.dumps(tempUnits, indent=4))

        # df = pd.DataFrame()
        self.dei_Float['DATA'] = pd.json_normalize(tempUnits)
        #print(self.dei_Float)

        #Now, get the us-gaap part
        temp = data_details[self.key_facts_gaap].keys()

        #This is a list of all the factors.
        self.data_companyfacts = data_details[self.key_facts_gaap]
        print(pd.json_normalize(self.data_companyfacts, max_level=1))
        #Generate table to make it easy to review
        # rt = rTable(box=box.DOUBLE)
        # rt.add_column('Label')
        # rt.add_column('Description')
        # rtB = rTable(box=box.DOUBLE)
        # rtB.add_column('Label')
        # rtB.add_column('Description')
        # cons = rConsole(record=True)
        # for idx, t in enumerate(temp):
        #     #cons.print('{}: {}'.format(t, data_details[self.key_facts_gaap][t][self.key_description]))
        #     if idx % 2 == 0:
        #         st = 'on white'
        #     else:
        #         st = ''
        #     desc = data_details[self.key_facts_gaap][t][self.key_description]
        #     rt.add_row(*[t, desc], style=st)
        #     if desc != None:
        #         if 'balance sheet' in desc:
        #             rtB.add_row(*[t, desc], style=st)
        #
        # #cons.print(rt)
        # #cons.print(rtB)
        # #cons.save_html('temp.html')





        #Now, break down tempUnits and create a panda data frame for easier analysis


        #print(json.dumps(data_details[self.key_facts_dei][self.key_dei_shares], indent=4))

        #elf.deiTable = rTable()





        #print(json.dumps(data_cik, indent=4))


        return True

    def getCompanyFactsAll(self):
        #Return list of all facts in accounting
        return self.data_companyfacts.keys()

    def _dataFileName(self):
        if not self.tickerExists:
            print('Ticker is not instantiated yet.  Exiting...')
            exit()

        #Ticker exists, create file name
        fnamebase =  self.analysisFolder + '/' + self.ticker + '_flatteneddata'
        fname = {'highlevel':fnamebase + '_highlevel.csv', 'details': fnamebase + '_details.csv'}

        return fname

    #Create several dataframes
    def _processGAAPdata(self):
        '''This does some post processing on the data, and flattens the JSON into several panda dataframes'''

        keys = self.getCompanyFactsAll() #Get all company facts
        fname = self._dataFileName()
        #Step one:  Create dataframe with companyfact, label, description,and number of units
        df = pd.DataFrame({'companyfact':[], 'label':[], 'description': [], 'units': []})
        for k in keys:
            label = self.data_companyfacts[k]['label']
            desc = self.data_companyfacts[k]['description']
            units = self.data_companyfacts[k]['units'].keys()
            print('{}, {}, {}, {}'.format(k, label, desc, units))
            df2 = pd.DataFrame({'companyfact': [k],
                                'label': [label],
                                'description': [desc],
                                'units': [units]})
            #print(df2)
            df = pd.concat([df, df2], ignore_index=True)

        self.df_companyfacts_highlevel = df #High level details of company facts
        self.df_companyfacts_highlevel.to_csv(fname['highlevel'])

        #Step 2: create one with company facts, accn, val, fy, fp, form
        df = pd.DataFrame({'companyfact': [], 'unit': [], 'accn': [], 'val': [], 'form': [], 'fy': [], 'fp': []})
        for k in keys:
            units = self.data_companyfacts[k]['units'].keys()
            for u in units:
                temp = self.data_companyfacts[k]['units'][u]
                #Can analyze this data now
                for t in temp:
                    #print(t)
                    val = t['val']
                    accn = t['accn']
                    form = t['form']
                    fy = t['fy']
                    fp = t['fp']
                    df2= pd.DataFrame(
                        {'companyfact': [k],
                         'unit': [u],
                         'accn': [accn],
                         'val': [val],
                         'form': [form],
                         'fy': [fy],
                         'fp': [fp]})

                    df = pd.concat([df, df2], ignore_index=True)

        self.df_companyfacts_details = df
        self.df_companyfacts_details.to_csv(fname['details'])
        #print(df)



    def getCompanyFacts(self, form, fy, fp):
        '''Returns subset of facts that apply for fy, fp, and form
        fy: fiscal year.
        fp: fiscal quarter (FY) is year
        form: which form'''

        #Should return fact, value, and apsh number
        print('In get company factss')
        if not self.isProcessed:
            self._processGAAPdata()
            self.isProcessed = True

        #Validate form
        if form not in self.form_types:
            print('ERROR: Form is not in the accepted list of {}.  Exiting ... '.format(self.form_types))
            exit()

        #Else, narrow down list with form, fy, and fp

        df_reduced = self.df_companyfacts_details[(self.df_companyfacts_details['form'] == form)
                                                  & (self.df_companyfacts_details['fy'] == fy)
                                                  & (self.df_companyfacts_details['fp'] == fp)]
        print(df_reduced)

