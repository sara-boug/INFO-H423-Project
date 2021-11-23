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


def map_time(dataset):
    return dataset['time']


def map_lines(responses):
    return responses['lines']

"""
def extract_only_same_pointId(positions_list):
    res = []
    for obj in positions_list:
        for i in obj:
            if i['pointId'] in list(map(get_pointId , [item for sublist
             in positions_list for item in sublist])):
                res.append(i)

    return res
"""
# getting 2 timestamp (2 calls)
with open("./splited_json/splited_json/vehiclePosition13.json", "r") as read_file:
    data = json.load(read_file)


testlist = data['data'][0:80]
"""
f = open("./splited_json/splited_json/demofile2.txt", "a")
f.write(str(testlist))
f.close()
"""
li_time = list(map(map_time, testlist))

def map_lineIDs(lines):
    return {'lineId':lines['lineId'],'SpeedLineId':[]}
def map_lines(responses):
    li = [item  for res in responses for item in res['lines']]

    return list(map(map_lineIDs, li)) 

speed_list = map_lines(testlist[0]['Responses']) # Prepare a list of lineID with it list of speed over timestamps calls    
for idx, call in enumerate(testlist): # loop in all timestamp responses
    time_call = call['time']# get the timestamp
    api_lines_responses = [item for resp in
     call['Responses'] if resp is not None for item in resp['lines']]
      #Since the API calls is recorded sequentualy, map all lineIDs within all lines responses in one list with skipping None responses
    for line in api_lines_responses:
        # get vehicle position list for each line
        
        if idx == 0 : # case when we are in the first timestamp, compute the speed = distanceFromPointID/ 30 secondes
            speed_first_api_call = [pos['distanceFromPoint']/30 for pos in
             line['vehiclePositions']] 
            #print(">>>>>>>>>>>>>\n" , speed_first_distance)
        else: # general cases when we are in Timestamp Ti
            last_time_call = testlist[idx-1]['time'] # get the last timestamp
            speed_stop_list = [] #at the end of the loop, this list contain all speed in all stop station
            for vehicle_position in line['vehiclePositions']: # loop on all vehicle position per lineID
                map_last_distance = [pos for resp in testlist[idx-1]['Responses']
                 if resp is not None for item in resp['lines'] for pos in
                  item['vehiclePositions']] # this map the last timestamp distances for all pointID 
                for di in map_last_distance: # loop on all last distance in order to compare direction, pointID, and distance i should be >distance i-1
                    if vehicle_position['directionId'] == di['directionId'] and vehicle_position['pointId'] == di['pointId'] and vehicle_position['distanceFromPoint'] >= di['distanceFromPoint']:  
                        # thischeck get the most relevant data to compute speed
                        speed = (vehicle_position['distanceFromPoint'] -
                         di['distanceFromPoint'])/(int(time_call) - int(last_time_call)) 
                        speed_stop_list.append(speed) # add new stop speed
                        break
    

        for li in speed_list:# loop in the prepared list of speed per line and update it with new speed
                if li['lineId'] == line['lineId']:
                    if idx == 0 :
                        li['SpeedLineId'].append(statistics.median(speed_first_api_call))## get the reel stat mean after!!
                    elif  speed_stop_list:
                        li['SpeedLineId'].append(statistics.median(speed_stop_list))
                    break
                    #speed_per_line.append(statistics.mean(speed_per_line))
#print(">>>>>>>>this is the final \n", speed_list)
            
    # case when we are in the first tipe point >>> idx = 0
    




# Ploting


plt.plot(li_time,speed_list[0]['SpeedLineId'])
plt.show()
