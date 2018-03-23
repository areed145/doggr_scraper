#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:28:51 2018

@author: areed145
"""
from queue import Queue
from threading import Thread
import pandas as pd
import numpy as np
import requests
import re
import datetime

d = pd.read_csv('AllWells_20180131.csv')
apis = d['API'].copy(deep=True)
apis.sort_values(inplace=True, ascending=True)
apistodo = apis
apistodo = apis[(apis >= 2926474) & (apis <= 2926490)]
columns = ['api', 'lease', 'well', 'county', 'countycode', 'district', 'operator',
           'operatorcode', 'field', 'fieldcode', 'area', 'areacode', 'section', 
           'township', 'rnge', 'bm', 'wellstatus', 'pwt', 'spuddate', 'gissrc', 
           'elev', 'latitude', 'longitude', 'date', 'oil', 'water', 'gas', 
           'daysprod', 'oilgrav', 'pcsg', 'ptbg', 'btu', 'method', 'waterdisp', 
           'pwtstatus_p', 'welltype_p', 'status_p', 'poolcode_p', 'wtrstm', 
           'gasair', 'daysinj', 'pinjsurf', 'wtrsrc', 'wtrknd', 'pwtstatus_i', 
           'welltype_i', 'status_i', 'poolcode_i']

class DownloadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            api, percent, ct = self.queue.get()
            url = 'https://secure.conservation.ca.gov/WellSearch/Details?api='+'{num:08d}'.format(num=api)
            print(url+', '+str(ct)+', '+str(percent))
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
            hh = pd.DataFrame(columns=columns, index=[1])
            hh['lease'] = lease
            hh['well'] = well
            hh['county'] = county
            hh['countycode'] = countycode
            hh['district'] = district
            hh['operator'] = operator
            hh['operatorcode'] = operatorcode
            hh['field'] = field
            hh['fieldcode'] = fieldcode
            hh['area'] = area
            hh['areacode'] = areacode
            hh['section'] = section
            hh['township'] = township
            hh['rnge'] = rnge
            hh['bm'] = bm
            hh['wellstatus'] = wellstatus
            hh['pwt'] = pwt
            hh['spuddate'] = spuddate
            hh['gissrc'] = gissrc
            hh['elev'] = elev
            hh['latitude'] = latitude
            hh['longitude'] = longitude
            hh['api'] = '{num:08d}'.format(num=api)
            prod = re.findall('{\"Production+(.*?)}', page)
            pp = pd.DataFrame()
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
                if len(prod)>0:
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
                    if re.findall('YearlySum":+(.*?),', i)[0] != 'true':
                        pp = pp.append(p).replace('null', np.nan, regex=True)
            inj = re.findall('{\"Injection+(.*?)}', page)
            ii = pd.DataFrame()
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
                if len(inj)>0: 
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
                if re.findall('YearlySum":+(.*?),', i)[0] != 'true':
                    ii = ii.append(j).replace('null', np.nan, regex=True)
            data = hh
            if len(pp)>0:
                if len(ii)>0:
                    pi = pp.merge(ii, how='outer', on=hh.columns+['date'])
                    data = data.merge(pi, how='outer', on=columns)
                else:
                    data = data.merge(pp, how='outer', on=columns)
            else:
                if len(ii)>0:
                    data = data.merge(ii, how='outer', on=columns)
                else:
                    pass            
            data = data[columns]
            data = data.convert_objects(convert_dates=False, convert_numeric=True)
            data.to_csv('doggr/'+str(api)+'.csv', index=False)
            self.queue.task_done()
            
def main():
    queue = Queue()
    for x in range(20):
        worker = DownloadWorker(queue)
        worker.daemon = True
        worker.start()
    for idx, api in enumerate(apistodo):
        percent = np.round(100 * idx / len(apistodo),2)
        queue.put((api, percent, idx+1))
    queue.join()
    
main()
#datas.to_gbq('doggr.t_doggr_prodinj', 'kk6gpv', if_exists='append')