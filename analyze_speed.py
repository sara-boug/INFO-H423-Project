import json
import time
from datetime import datetime, timedelta
import numpy as np

import matplotlib.pyplot as plt
import pandas as pd
import statistics

"""
.Data preparation steps
    - time is an eposh type >>>> cast to timestamp
    - data is organized in json file convert to python
    - the data is it ordred or not?
    - we do not need triple that have 0 distance?
.
"""
# read json file


def oneway_direction(positions_list, directionId):
    res = []
    for obj in positions_list:
        li = [ i for i in obj if i['directionId'] == directionId]
        res.append(li)
    return res

def get_pointId(vehiclePosition):
    return vehiclePosition['pointId']

def map_time(dataset):
    return dataset['time']


def map_responses(dataset):
    return dataset['Responses']

def map_lines(responses):
    return responses['lines']

def extract_only_same_pointId(positions_list):
    res = []
    for obj in positions_list:
        for i in obj:
            if i['pointId'] in list(map(get_pointId , [item for sublist in positions_list for item in sublist])):
                res.append(i)

    return res

# getting 2 timestamp (2 calls)
with open("./splited_json/splited_json/vehiclePosition13.json", "r") as read_file:
    data = json.load(read_file)

testlist = data['data'][0:100]
li_time = list(map(map_time, testlist))

def map_lineIDs(lines):
    return {'lineId':lines['lineId'],'SpeedLineId':[]}
def map_lines(responses):
    li = [item  for res in responses for item in res['lines']]

    return list(map(map_lineIDs, li)) 

speed_list = map_lines(testlist[0]['Responses'])    
for idx, call in enumerate(testlist) :
    time_call = call['time']
    api_lines_responses = [item  for resp in call['Responses']  for item in resp['lines']]
    for line in api_lines_responses:
         # get vehicle position list for each line
        
        if idx == 0 :
            speed_first_distance = [pos['distanceFromPoint']/30 for pos in line['vehiclePositions']] 

        else:
            last_time_call = testlist[idx-1]['time']
            speed_stop_list = []
            for vehicle_position in line['vehiclePositions']:
                map_last_distance = [pos for resp in testlist[idx-1]['Responses'] for item in resp['lines'] for pos in item['vehiclePositions']] 
                for di in map_last_distance:
                    if vehicle_position['directionId'] == di['directionId'] and vehicle_position['pointId'] == di['pointId'] and vehicle_position['distanceFromPoint'] >= di['distanceFromPoint']:  
                        # this is the best data check
                        speed = (vehicle_position['distanceFromPoint'] - di['distanceFromPoint']) /(int(time_call) - int(last_time_call)) 
                        speed_stop_list.append(speed)
                        break
    

        for li in speed_list:
                if li['lineId'] == line['lineId']:
                    if idx == 0 :
                        li['SpeedLineId'].append(0.0)## get the reel stat mean after!!
                    elif  speed_stop_list :
                        li['SpeedLineId'].append(statistics.mean(speed_stop_list))
                    else :
                        li['SpeedLineId'].append(0.0)
                    break
                    #speed_per_line.append(statistics.mean(speed_per_line))
print(">>>>>>>>this is the final \n",speed_list)
            
    # case when we are in the first tipe point >>> idx = 0
    
"""

 for line in lines:
    if pointId in map list before 
        then >>>  speed = di - di-1/ ti-ti-1
    else : speed = 0        
"""



"""
# Ploting

speed= range(7,10)
print(t0,t1,t2)
plt.plot(times,speed)
plt.show()
"""