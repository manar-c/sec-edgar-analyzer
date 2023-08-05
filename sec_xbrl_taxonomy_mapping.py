#This class analyzes the taxonomies and generates a mapping to the different financial statements
#I have not seen a clean way of doing this yet, with a clear mapping
#This uses several files the SEC provides every quarter, that map the company concepts to the
#different financial statements

#Created on 7/30/2023

from urllib.request import Request, urlopen
import os
import zipfile
import pandas as pd
import shutil
class XBRL_MAPPING:
    def __init__(self, folder='download', last='2023q2', first='2009q1', forceDownload=False):
        ''' folder: where to save.  last & first: last and first file to download.  forceDownload: don't download if already exists'''
        #self.initialized = False
        self.forceDownload = forceDownload
        #Main link here: https://www.sec.gov/dera/data/financial-statement-data-sets
        self.targetURL = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/#'
        self.folder = folder
        temp = first.split('q')
        self.first = first
        self.last = last
        self.startYear = int(temp[0])
        self.startQ = int(temp[1])
        temp = last.split('q')
        self.endYear = int(temp[0])
        self.endQ = int(temp[1])

        self.totalQ = 4 #Total number of quarters in a year

        #Key files to analyze
        self.f_num = 'num.txt'
        self.f_pre = 'pre.txt'
        self.f_sub = 'sub.txt'
        self.f_tag = 'tag.txt'

    def _getFileList(self):
        fList = []
        for y in range(self.startYear, self.endYear + 1):
            if y == self.startYear:
                sQ = self.startQ  # Starting quarter
            else:
                sQ = 1  # Start from first quarter
            if y == self.endYear:
                eQ = self.endQ
            else:
                eQ = 4  # End at fourth quarter
            for q in range(sQ, eQ + 1):
                fList.append(str(y) + 'q' + str(q) + '.zip')

        return fList

    def downloadAll(self):
        fList = self._getFileList()
        for f in fList:
            url = self.targetURL.replace('#', f)
            print(url)
            if os.path.exists('{}/{}'.format(self.folder, f)) and not self.forceDownload:
                continue #Do not waste time downloading

            z1 = Request(url)
            if not os.path.exists(self.folder):
                os.makedirs(self.folder)

            with urlopen(z1) as response, open('{}/{}'.format(self.folder, f), 'wb') as out_file:
                data = response.read()  # a `bytes` object
                out_file.write(data)

            with zipfile.ZipFile('{}/{}'.format(self.folder, f), 'r') as zObject:
                # Extracting all the members of the zip
                # into a specific location.
                zObject.extractall(path='{}/{}'.format(self.folder, f.split('.')[0]))

    def _unzipAll(self):
        ''' Unzips all files '''
        fList = self._getFileList()
        for f in fList:
            with zipfile.ZipFile('{}/{}'.format(self.folder, f), 'r') as zObject:
                # Extracting all the members of the zip
                # into a specific location.
                zObject.extractall(path='{}/{}'.format(self.folder, f.split('.')[0]))

    def _deleteAllunZipped(self):
        ''' Unzips all files '''
        fList = self._getFileList()
        for f in fList:
            fol = f.split('.zip')[0]  # Remove zip, just look at folder
            shutil.rmtree(self.folder + '/' + fol)
    def analyzeAll(self):
        ''' Analyzes downloaded files and stores key parameters to reduce size into a dataframe.
         Save the following.  1) CIK. 2) ADSH. 3) TAG 4) STMT. 5) datatype (null or monetary, from TAG) 6) crdr (if monetary, either credit
         or debit (also from TAG)'''

        #First, unzip all
        self._unzipAll()

        fList = self._getFileList()
        df_sub_main = pd.DataFrame({'cik':[], 'adsh':[]})
        df_pre_main = pd.DataFrame({'adsh':[], 'tag':[], 'stmt':[]})
        df_tag_main = pd.DataFrame({'tag': [], 'version': [], 'abstract': [], 'datatype': [], 'crdr':[]}) #['tag', 'version', 'abstract', 'datatype', 'crdr']
        for f in fList:



            fol = f.split('.zip')[0] #Remove zip, just look at folder
            path = '{}/{}'.format(self.folder, fol)
            print('Analyzing folder {}'.format(path))


            # The readme.htm file in the unzipped location has quite a bit of info
            # Key files to look at: 1) sub.txt --> contains CIK and ADSH.  Contains other general information, not sure the rest is useful
            # tag.txt contains all the taxonomy tags, and whether they are custom or not.  Not sure I are about that right now
            # pre.txt contains adsh, tag, and stmt.  Can have two stms.  ALPHANUMERIC (BS = Balance Sheet, IS = Income Statement, CF = Cash Flow, EQ = Equity, CI = Comprehensive Income, UN = Unclassifiable Statement).
            # Not sure I need num.txt?

            df_sub = pd.read_csv(path + '/' + 'sub.txt', sep='\t')
            #print(targetCIK)
            df_sub_red = df_sub.filter(items=['cik', 'adsh'])
            #print(df_sub_red)
            df_sub_main = pd.concat([df_sub_main, df_sub_red], ignore_index=True)
            #red = df_sub[df_sub.cik == targetCIK['cik']]
            #print(red)

            # Load pre .txt

            df_pre = pd.read_csv(path + '/' + 'pre.txt', sep='\t')
            df_pre_red = df_pre.filter(items=['adsh', 'tag', 'stmt'])
            df_pre_main = pd.concat([df_pre_main, df_pre_red], ignore_index = True)

            #Load tag.txt
            df_tag = pd.read_csv(path + '/' + 'tag.txt', sep='\t')
            df_tag_red = df_tag.filter(items=['tag', 'version', 'abstract', 'datatype', 'crdr'])
            df_tag_main = pd.concat([df_tag_main, df_tag_red], ignore_index=True)
            #print(df_pre.columns)
            #pd.set_option('display.max_columns', None)
            #pd.set_option('display.max_rows', None)
            #temp = []
        print(df_sub_main)
        print(df_pre_main)
        fname = self.folder + '/' + self.first + '_' + self.last + '_sub.csv'
        df_sub_main.to_csv(fname)
        fname = self.folder + '/' + self.first + '_' + self.last + '_pre.csv'
        df_pre_main.to_csv(fname)
        fname = self.folder + '/' + self.first + '_' + self.last + '_tag.csv'
        df_tag_main.to_csv(fname)

        #Now, delete all unzipped folders
        self._deleteAllunZipped()



if __name__ == '__main__':
    xbrl = XBRL_MAPPING(folder='download')#, last='2023q2', first='2012q1')#, last='2011q1')
    xbrl.downloadAll()
    xbrl.analyzeAll()