#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:28:51 2018

@author: areed145
"""
import pandas as pd
import numpy as np
import requests
import re
import datetime

d = pd.read_csv('AllWells_20180131.csv')
apis = d['API'].copy(deep=True)
apis.sort_values(inplace=True, ascending=False)
apistodo = apis
#apistodo = apis[(apis >= 3714300) & (apis <= 3714400)]
columns = ['api', 'lease', 'well', 'county', 'countycode', 'district', 'operator',
           'operatorcode', 'field', 'fieldcode', 'area', 'areacode', 'section', 
           'township', 'rnge', 'bm', 'wellstatus', 'pwt', 'spuddate', 'gissrc', 
           'elev', 'latitude', 'longitude', 'date', 'oil', 'water', 'gas', 
           'daysprod', 'oilgrav', 'pcsg', 'ptbg', 'btu', 'method', 'waterdisp', 
           'pwtstatus_p', 'welltype_p', 'status_p', 'poolcode_p', 'wtrstm', 
           'gasair', 'daysinj', 'pinjsurf', 'wtrsrc', 'wtrknd', 'pwtstatus_i', 
           'welltype_i', 'status_i', 'poolcode_i']
datas = pd.DataFrame()
count = 0
for idx, api in enumerate(apistodo):
    percent = np.round(100 * idx / len(apistodo),2)
    url = 'https://secure.conservation.ca.gov/WellSearch/Details?api='+'{num:08d}'.format(num=api)
    print(url+', '+str(percent))
    page = requests.get(url).text 
    lease = re.findall('Lease</label> <br />\s*(.*?)\s*</div', page)[0]
    well = re.findall('Well #</label> <br />\s*(.*?)\s*</div', page)[0]
    county = re.findall('County</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][0]
    countycode = re.findall('County</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][1]
    district = int(re.findall('District</label> <br />\s*(.*?)\s*</div', page)[0])
    operator = re.findall('Operator</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][0]
    operatorcode = re.findall('Operator</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][1]
    field = re.findall('Field</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][0]
    fieldcode = re.findall('Field</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][1]
    area = re.findall('Area</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][0]
    areacode = re.findall('Area</label> <br />\s*(.*)<span>\s\[(.*)\]\s*</span>', page)[0][1]
    section = re.findall('Section</label><br />\s*(.*?)\s*</div', page)[0]
    township = re.findall('Township</label><br />\s*(.*?)\s*</div', page)[0]
    rnge = re.findall('Range</label><br />\s*(.*?)\s*</div', page)[0]
    bm = re.findall('Base Meridian</label><br />\s*(.*?)\s*</div', page)[0]
    wellstatus = re.findall('Well Status</label><br />\s*(.*?)\s*</div', page)[0]
    pwt = re.findall('Pool WellTypes</label> <br />\s*(.*?)\s*</div', page)[0]
    spuddate = re.findall('SPUD Date</label> <br />\s*(.*?)\s*</div', page)[0]
    gissrc = re.findall('GIS Source</label> <br />\s*(.*?)\s*</div', page)[0]
    elev = re.findall('Datum</label> <br />\s*(.*?)\s*</div', page)[0]
    latitude = re.findall('Latitude</label> <br />\s*(.*?)\s*</div', page)[0]
    longitude = re.findall('Longitude</label> <br />\s*(.*?)\s*</div', page)[0]                  
    prod = re.findall('{\"Production+(.*?)}', page)
    pp = pd.DataFrame(columns=columns)
    if len(prod)>0:
        for idx, i in enumerate(prod):
            p = pd.DataFrame(index=[re.findall('Date\(+(.*?)\)', i)[0]])
            p['lease'] = lease
            p['well'] = well
            p['county'] = county
            p['countycode'] = countycode
            p['district'] = district
            p['operator'] = operator
            p['operatorcode'] = operatorcode
            p['field'] = field
            p['fieldcode'] = fieldcode
            p['area'] = area
            p['areacode'] = areacode
            p['section'] = section
            p['township'] = township
            p['rnge'] = rnge
            p['bm'] = bm
            p['wellstatus'] = wellstatus
            p['pwt'] = pwt
            p['spuddate'] = spuddate
            p['gissrc'] = gissrc
            p['elev'] = elev
            p['latitude'] = latitude
            p['longitude'] = longitude
            p['api'] = '{num:08d}'.format(num=api)
            p['date'] = datetime.datetime.fromtimestamp(int(re.findall('Date\(+(.*?)\)', i)[0][:-3])).strftime('%Y-%m-%d')
            p['oil'] = re.findall('OilProduced":+(.*?),', i)[0]
            p['water'] = re.findall('WaterProduced":+(.*?),', i)[0]
            p['gas'] = re.findall('GasProduced":+(.*?),', i)[0]
            p['daysprod'] = re.findall('NumberOfDaysProduced":+(.*?),', i)[0]
            p['oilgrav'] = re.findall('OilGravity":+(.*?),', i)[0]
            p['pcsg'] = re.findall('CasingPressure":+(.*?),', i)[0]
            p['ptbg'] = re.findall('TubingPressure":+(.*?),', i)[0]
            p['btu'] = re.findall('BTU":+(.*?),', i)[0]
            p['method'] = re.findall('MethodOfOperation":+(.*?),', i)[0].replace('"', '')
            p['waterdisp'] = re.findall('WaterDisposition":+(.*?),', i)[0].replace('"', '')
            p['pwtstatus_p'] = re.findall('PWTStatus":+(.*?),', i)[0].replace('"', '')
            p['welltype_p'] = re.findall('WellType":+(.*?),', i)[0].replace('"', '')
            p['status_p'] = re.findall('Status":+(.*?),', i)[0].replace('"', '')
            p['poolcode_p'] = re.findall('PoolCode":+(.*?),', i)[0].replace('"', '')
            if re.findall('YearlySum":+(.*?),', i)[0] == 'false':
                pp = pp.append(p).replace('null', np.nan, regex=True)
    inj = re.findall('{\"Injection+(.*?)}', page)
    ii = pd.DataFrame(columns=columns)
    if len(inj)>0:   
        for idx, i in enumerate(inj):
            j = pd.DataFrame(index=[re.findall('Date\(+(.*?)\)', i)[0]])
            j['lease'] = lease
            j['well'] = well
            j['county'] = county
            j['countycode'] = countycode
            j['district'] = district
            j['operator'] = operator
            j['operatorcode'] = operatorcode
            j['field'] = field
            j['fieldcode'] = fieldcode
            j['area'] = area
            j['areacode'] = areacode
            j['section'] = section
            j['township'] = township
            j['rnge'] = rnge
            j['bm'] = bm
            j['wellstatus'] = wellstatus
            j['pwt'] = pwt
            j['spuddate'] = spuddate
            j['gissrc'] = gissrc
            j['elev'] = elev
            j['latitude'] = latitude
            j['longitude'] = longitude
            j['api'] = '{num:08d}'.format(num=api)
            j['date'] = datetime.datetime.fromtimestamp(int(re.findall('Date\(+(.*?)\)', i)[0][:-3])).strftime('%Y-%m-%d')
            j['wtrstm'] = re.findall('WaterOrSteamInjected":+(.*?),', i)[0]
            j['gasair'] = re.findall('GasOrAirInjected":+(.*?),', i)[0]
            j['daysinj'] = re.findall('NumberOfDaysInjected":+(.*?),', i)[0]
            j['pinjsurf'] = re.findall('SurfaceInjectionPressure":+(.*?),', i)[0]
            j['wtrsrc'] = re.findall('SourceOfWater":+(.*?),', i)[0].replace('"', '')
            j['wtrknd'] = re.findall('KindOfWater":+(.*?),', i)[0].replace('"', '')
            j['pwtstatus_i'] = re.findall('PWTStatus":+(.*?),', i)[0].replace('"', '')
            j['welltype_i'] = re.findall('WellType":+(.*?),', i)[0].replace('"', '')
            j['status_i'] = re.findall('Status":+(.*?),', i)[0].replace('"', '')
            j['poolcode_i'] = re.findall('PoolCode":+(.*?),', i)[0].replace('"', '')
            if re.findall('YearlySum":+(.*?),', i)[0] == 'false':
                ii = ii.append(j).replace('null', np.nan, regex=True)
    if len(pp)>0:
        if len(ii)>0:
            data = pp.merge(ii, how='outer', on=columns)
        else:
            data = pp
    elif len(ii)>0:
        data = ii
    else:
        a = 1
    try:
        data = data[columns]
        datas = datas.append(data)
    except:
        pass
    count = count+1
    if count/500 == np.round(count/500,0):
        datas = datas.convert_objects(convert_dates=False, convert_numeric=True)
        datas.to_gbq('doggr.t_doggr_prodinj', 'kk6gpv', if_exists='append')
            