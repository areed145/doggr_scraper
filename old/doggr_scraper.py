# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import urllib

well_files = ['/Users/areed145/Dropbox/GitHub/doggr/CYM_Wells.xls',
              '/Users/areed145/Dropbox/GitHub/doggr/MCK_Wells.xls']
              
intvl = .5

marker = intvl

data_count = 0
              
imp = pd.DataFrame()
for file in well_files:
    imp = imp.append(pd.ExcelFile(file).parse('CA DOGGR Wells', header=3).iloc[4:,:25])

api_list = imp['API #']

api_len = len(api_list)

compile = pd.DataFrame()

for idx_api, api in enumerate(api_list):
    
    prog = 100*(idx_api+1)/api_len
    
    if prog > marker:
        print(str(marker)+'% complete, '+str(data_count)+' wells with data found, '+str(len(compile))+' rows, '+str(np.round(compile.memory_usage().sum()/1000000,2))+'MB')
        marker = marker+intvl
        compile.to_csv('/Users/areed145/Dropbox/GitHub/doggr/CYMMCK_doggr_prod.csv', index=False)
        
    api = str(api)

    prod_url = 'https://secure.conservation.ca.gov/WellSearch/Download/ToExcelProductionData?APInumber=0'+api
    inj_url = 'https://secure.conservation.ca.gov/WellSearch/Download/ToExcelInjectionData?APInumber=0'+api
    
    prod = pd.ExcelFile(urllib.request.urlopen(prod_url))\
            .parse('Well Production', header=None)
    inj = pd.ExcelFile(urllib.request.urlopen(inj_url))\
            .parse('Well Injection', header=None)
    
    header = {}
    for idx_hdr, data in enumerate(prod.iloc[0,:15]):
        header[data] = prod.iloc[1][idx_hdr]
        
    prod_dates = pd.DataFrame(data=prod.iloc[4:,1].unique())
    inj_dates = pd.DataFrame(data=inj.iloc[4:,1].unique())
    dates = pd.DataFrame(data=prod_dates.append(inj_dates)[0].unique())
    dates = dates[~dates[0].str.contains('Total')]
    
    prod_pool = pd.DataFrame(data=prod.iloc[4:,15].unique())
    inj_pool = pd.DataFrame(data=inj.iloc[4:,11].unique())
    pools = pd.DataFrame(data=prod_pool.append(inj_pool)[0].unique())
    pools = pools[pd.notnull(pools[0])]
        
    prodinj = pd.DataFrame(data=dates[0], columns=['Month'])
    for col in ['Field Name',
                'Latitude',
                'Lease Name',
                'Longitude',
                'Operator Name',
                'Range',
                'Section',
                'Township',
                'Well #']:
        prodinj[col] = header[col]
        prodinj['API10'] = '04'+str(header['API #'])
        
    prod_used = False
    inj_used = False
    
    if len(prod_dates) > 0:
        prod_data = prod.iloc[4:,:17]
        prod_data.columns = prod.iloc[3,:17]
        prod_data['API12'] = '04'+header['API #']+prod_data['Pool Code']
        prod_data = prod_data.drop(['Status', 'Pool Code', 'API Number', 'Gravity of Oil', 'Casing Pressure', 'Tubing Pressure', 'BTU', 'Method of Operation', 'Water Disposition', 'PWT Status', 'Well Type', 'Reported Date'], axis=1)
        prodinj = prodinj.merge(prod_data, how='left', left_on=['Month'], right_on=['Production Date'])
        prodinj = prodinj.drop(['Production Date'], axis=1)
        prod_used = True
        
    if len(inj_dates) > 0:
        inj_data = inj.iloc[4:,:13]
        inj_data.columns = inj.iloc[3,:13]
        inj_data['API12'] = '04'+header['API #']+inj_data['Pool Code']
        inj_data = inj_data.drop(['Status', 'Pool Code', 'API Number', 'Surface Injection Pressure', 'Source of Water', 'Kind of Water', 'PWT Status', 'Well Type', 'Reported Date'], axis=1)
        if prod_used == True:
            prodinj = prodinj.merge(inj_data, how='left', left_on=['Month','API12'], right_on=['Injection Date','API12'])
        else:
            prodinj = prodinj.merge(inj_data, how='left', left_on=['Month'], right_on=['Injection Date'])
        prodinj = prodinj.drop(['Injection Date'], axis=1)
        inj_data = True
        
    for product in ['Days Well Injected', 'Days Well Produced', 'Gas Produced (Mcf)', 'Gas or Air Injected (Mcf)', 'Oil Produced (bbl)', 'Water Produced (bbl)', 'Water or Steam Injected (bbl)']:
        try:
            prodinj[product] = prodinj[product].fillna(0)
        except:
            pass
        
    if prod_used == True:
        if inj_used == True:
            data_count = data_count + 1
            prodinj = prodinj[(prodinj['Days Well Injected'] > 0) | (prodinj['Days Well Produced'] > 0)]
        else:
            data_count = data_count + 1
            prodinj = prodinj[prodinj['Days Well Produced'] > 0]
    else:
        if inj_used == True:
            data_count = data_count + 1
            prodinj = prodinj[prodinj['Days Well Injected'] > 0]
        else:
            data_count = data_count
            
    prodinj['Month'] = pd.to_datetime(prodinj['Month'], format='%b-%Y')

    compile = compile.append(prodinj)
    
        
    