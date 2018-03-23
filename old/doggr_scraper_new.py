# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import glob
import urllib
import os

getlist = {}
compiler = {}
fetch = {}

compiler['file_name_appendix'] = 'bv'
compiler['intvl'] = 5
compiler['limit'] = 15

compiler['marker'] = compiler['intvl']
compiler['file'] = 0

compiler['size'] = 0
compiler['count'] = 0
compiler['rows'] = 0
compiler['size_rem'] = 0
compiler['count_rem'] = 0
compiler['rows_rem'] = 0

# sample inputs
#getlist['fieldName'] = 'Cymric'
#getlist['fieldName'] = 'Buena Vista'
#getlist['operatorCode'] = None
#getlist['fieldName'] = 'Midway-Sunset'

# inputs
getlist['fieldName'] = None
getlist['districtNumber'] = None
getlist['county'] = None
getlist['operatorCode'] = 'L2025'
getlist['leaseName'] = None
getlist['apiNum'] = None
getlist['activeWellsOnly'] = False
getlist['addr'] = None
getlist['loc'] = None
getlist['sec'] = None
getlist['twn'] = None
getlist['rge'] = None
getlist['bm'] = None

filenames = glob.glob('C:/Users/bvjs/Python/python-3.4.3.amd64/bvjs/doggr_scraper/cymmck_doggr_prod_*.pk1')
for file_ in filenames:
    os.remove(file_)

def compile_save(compile):

    compile['Cyclic Injected (bbl)'] = compile['Water or Steam Injected (bbl)']
    compile['Continous Injected (bbl)'] = compile['Water or Steam Injected (bbl)']
    compile['Water Injected (bbl)'] = compile['Water or Steam Injected (bbl)']

    compile.ix[compile['Well Type Inj'] != 'SC', 'Cyclic Injected (bbl)'] = 0
    compile.ix[compile['Well Type Inj'] != 'SF', 'Continous Injected (bbl)'] = 0
    compile.ix[compile['Well Type Inj'] != 'WD', 'Water Injected (bbl)'] = 0

    compile['Total Steam Injected (bbl)'] = compile['Cyclic Injected (bbl)'] + compile['Continous Injected (bbl)']

    compile = compile.drop(['Water or Steam Injected (bbl)'], axis=1)
    compile = compile.drop(['Gas or Air Injected (Mcf)'], axis=1)

    for compiler['product'] in ['Days Well Injected', 'Days Well Produced', 'Gas Produced (Mcf)', 'Oil Produced (bbl)', 'Water Produced (bbl)', 'Water Injected (bbl)', 'Continous Injected (bbl)', 'Cyclic Injected (bbl)', 'Total Steam Injected (bbl)']:
        try:
            compile[compiler['product']] = compile[compiler['product']].fillna(0)
        except:
            pass

    compile.to_pickle('C:/Users/bvjs/Python/python-3.4.3.amd64/bvjs/doggr_scraper/cymmck_doggr_prod_'+str(compiler['file'])+'.pk1')

def build_url():
    getlist['url'] = 'https://secure.conservation.ca.gov/WellSearch/Download/ExportWellSearchResults?'\
                +'districtNumber='+getlist['districtNumber']\
                +'&county='+getlist['county']\
                +'&fieldName='+getlist['fieldName']\
                +'&operatorCode='+getlist['operatorCode']\
                +'&leaseName='+getlist['leaseName']\
                +'&apiNum='+getlist['apiNum']\
                +'&activeWellsOnly='+getlist['activeWellsOnly']\
                +'&addr='+getlist['addr']\
                +'&loc='+getlist['loc']\
                +'&sec='+getlist['sec']\
                +'&twn='+getlist['twn']\
                +'&rge='+getlist['rge']\
                +'&bm='+getlist['bm']
                
getlist['districtNumber']=(str(getlist['districtNumber']) if getlist['districtNumber'] != None else '')
getlist['county']=(str(getlist['county']).replace(" ", "%20") if getlist['county'] != None else '')
getlist['fieldName']=(str(getlist['fieldName']).replace(" ", "%20") if getlist['fieldName'] != None else '')
getlist['operatorCode']=(str(getlist['operatorCode']) if getlist['operatorCode'] != None else '')
getlist['leaseName']=(str(getlist['leaseName']).replace(" ", "%20") if getlist['leaseName'] != None else '')
getlist['apiNum']=(str(getlist['apiNum']) if getlist['apiNum'] != None else '')
getlist['activeWellsOnly']=('false' if getlist['activeWellsOnly'] == False else 'true')
getlist['addr']=(str(getlist['addr']) if getlist['addr'] != None else '')
getlist['loc']=(str(getlist['loc']) if getlist['loc'] != None else '')
getlist['sec']=(str(getlist['sec']) if getlist['sec'] != None else '')
getlist['twn']=(str(getlist['twn']) if getlist['twn'] != None else '')
getlist['rge']=(str(getlist['rge']) if getlist['rge'] != None else '')
getlist['bm']=(str(getlist['bm']) if getlist['bm'] != None else '')

build_url()

getlist['apilist'] = pd.ExcelFile(urllib.request.urlopen(getlist['url']))\
            .parse('CA DOGGR Wells', header=None).iloc[4:,6].unique()
            
if getlist['fieldName'] == 'Cymric':
    getlist['fieldName'] = 'McKittrick'
    build_url()
    getlist['apilist'] = np.unique(np.append(getlist['apilist'], pd.ExcelFile(urllib.request.urlopen(getlist['url']))\
            .parse('CA DOGGR Wells', header=None).iloc[4:,6].unique()))

getlist['apilist_len'] = len(getlist['apilist'])

print(str(getlist['apilist_len'])+' unique wells found')

compile = pd.DataFrame()

for fetch['idx_api'], fetch['api'] in enumerate(getlist['apilist']):

    build = {}
    header = {}

    build['api'] = str(fetch['api'])

    build['prod_url'] = 'https://secure.conservation.ca.gov/WellSearch/Download/ToExcelProductionData?APInumber='+build['api']
    build['inj_url'] = 'https://secure.conservation.ca.gov/WellSearch/Download/ToExcelInjectionData?APInumber='+build['api']

    while True:
        try:
            build['prod'] = pd.ExcelFile(urllib.request.urlopen(build['prod_url']))\
                    .parse('Well Production', header=None)
            build['inj'] = pd.ExcelFile(urllib.request.urlopen(build['inj_url']))\
                    .parse('Well Injection', header=None)
        except:
            print('build fail: '+build['api'])
            continue
        break

    for build['idx_hdr'], build['data'] in enumerate(build['prod'].iloc[0,:15]):
        header[build['data']] = build['prod'].iloc[1][build['idx_hdr']]

    build['prod_dates'] = pd.DataFrame(data=build['prod'].iloc[4:,1].unique())
    build['inj_dates'] = pd.DataFrame(data=build['inj'].iloc[4:,1].unique())
    build['dates'] = pd.DataFrame(data=build['prod_dates'].append(build['inj_dates'])[0].unique())
    build['dates'] = build['dates'][~build['dates'][0].str.contains('Total')]

    build['prod_pool'] = pd.DataFrame(data=build['prod'].iloc[4:,15].unique())
    build['inj_pool'] = pd.DataFrame(data=build['inj'].iloc[4:,11].unique())
    build['pools'] = pd.DataFrame(data=build['prod_pool'].append(build['inj_pool'])[0].unique())
    build['pools'] = build['pools'][pd.notnull(build['pools'][0])]

    build['prodinj'] = pd.DataFrame(data=build['dates'][0], columns=['Month'])
    for build['col'] in ['Field Name',
                'Latitude',
                'Lease Name',
                'Longitude',
                'Operator Name',
                'Range',
                'Section',
                'Township',
                'Well #']:
        build['prodinj'][build['col']] = header[build['col']]
    build['prodinj']['API10'] = '04'+str(header['API #'])
    build['prodinj'] = build['prodinj'].rename(columns={'Well #':'Well'})

    build['prod_used'] = False
    build['inj_used'] = False

    if len(build['prod_dates']) > 0:
        build['prod_data'] = build['prod'].iloc[4:,:17]
        build['prod_data'].columns = build['prod'].iloc[3,:17]
        build['prod_data']['APIPC'] = '04'+header['API #']+build['prod_data']['Pool Code']
        build['prod_data'] = build['prod_data'].rename(columns={'Well Type':'Well Type Prod'})
        build['prod_data'] = build['prod_data'].drop(['Status', 'Pool Code', 'API Number', 'Gravity of Oil', 'Casing Pressure', 'Tubing Pressure', 'BTU', 'Method of Operation', 'Water Disposition', 'PWT Status', 'Reported Date'], axis=1)
        build['prodinj'] = build['prodinj'].merge(build['prod_data'], how='left', left_on=['Month'], right_on=['Production Date'])
        build['prodinj'] = build['prodinj'].drop(['Production Date'], axis=1)
        build['prod_used'] = True

    if len(build['inj_dates']) > 0:
        build['inj_data'] = build['inj'].iloc[4:,:13]
        build['inj_data'].columns = build['inj'].iloc[3,:13]
        build['inj_data']['APIPC'] = '04'+header['API #']+build['inj_data']['Pool Code']
        build['inj_data'] = build['inj_data'].rename(columns={'Well Type':'Well Type Inj'})
        build['inj_data'] = build['inj_data'].drop(['Status', 'Pool Code', 'API Number', 'Surface Injection Pressure', 'Source of Water', 'Kind of Water', 'PWT Status', 'Reported Date'], axis=1)
        if build['prod_used'] == True:
            build['prodinj'] = build['prodinj'].merge(build['inj_data'], how='left', left_on=['Month','APIPC'], right_on=['Injection Date','APIPC'])
        else:
            build['prodinj'] = build['prodinj'].merge(build['inj_data'], how='left', left_on=['Month'], right_on=['Injection Date'])
        build['prodinj'] = build['prodinj'].drop(['Injection Date'], axis=1)
        build['inj_used'] = True

    for build['product'] in ['Days Well Injected', 'Days Well Produced', 'Gas Produced (Mcf)', 'Gas or Air Injected (Mcf)', 'Oil Produced (bbl)', 'Water Produced (bbl)', 'Water or Steam Injected (bbl)']:
        try:
            build['prodinj'][build['product']] = build['prodinj'][build['product']].fillna(0)
        except:
            pass

    build['prodinj']['Month'] = pd.to_datetime(build['prodinj']['Month'], format='%b-%Y')

    build['prodinj']['Spud'] = pd.to_datetime(build['prodinj']['Month'].min(), format='%b-%Y')

    if build['prod_used'] == True:
        if build['inj_used'] == True:
            build['prodinj'] = build['prodinj'][(build['prodinj']['Days Well Injected'] > 0) | (build['prodinj']['Days Well Produced'] > 0)]
        else:
            build['prodinj'] = build['prodinj'][build['prodinj']['Days Well Produced'] > 0]
    else:
        if build['inj_used'] == True:
            build['prodinj'] = build['prodinj'][build['prodinj']['Days Well Injected'] > 0]

    build['prodinj'] = build['prodinj'].drop_duplicates()

    compile = compile.append(build['prodinj'])

    compiler['prog'] = 100*(fetch['idx_api']+1)/getlist['apilist_len']

    progress = compiler['prog']

    if compiler['prog'] > compiler['marker']:

        compiler['size'] = np.round(compile.memory_usage().sum()/1000000,2) + compiler['size_rem']
        compiler['count'] = len(compile['API10'].unique()) + compiler['count_rem']
        compiler['rows'] = len(compile) + compiler['rows_rem']

        print(str(np.round(compiler['prog'],1))+'% complete, '+str(compiler['count'])+' wells with data found, '+str(compiler['rows'])+' rows, '+str(compiler['size'])+'MB')
        compiler['marker'] = compiler['marker']+compiler['intvl']

        if np.round(compile.memory_usage().sum()/1000000,2) > compiler['limit']:

            compile_save(compile)

            compiler['size_rem'] = compiler['size']
            compiler['count_rem'] = compiler['count']
            compiler['rows_rem'] = compiler['rows']
            
            compile = pd.DataFrame()
            compiler['file'] = compiler['file'] + 1

print(str(np.round(compiler['prog'],1))+'% complete, '+str(compiler['count'])+' wells with data found, '+str(compiler['rows'])+' rows, '+str(compiler['size'])+'MB')
compile_save(compile)

print('compiling pickle files')

filenames = glob.glob('C:/Users/bvjs/Python/python-3.4.3.amd64/bvjs/doggr_scraper/cymmck_doggr_prod_*.pk1')
df_compiles = pd.DataFrame()
compiles = []
for file_ in filenames:
    df_compiles_ind = pd.read_pickle(file_)
    compiles.append(df_compiles_ind)
    os.remove(file_)
df_compiles = pd.concat(compiles)
del df_compiles_ind
df_compiles.to_csv('C:/Users/bvjs/Python/python-3.4.3.amd64/bvjs/doggr_scraper/cymmck_doggr_prod_'+compiler['file_name_appendix']+'.csv', index=False)

print('job complete!')
