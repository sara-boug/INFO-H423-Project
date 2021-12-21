import json
import time
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import statistics
from dateutil import parser
import csv
import glob

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




def duration_between_two_calls(t1, t2):
  time2 = parser.parse(t2)
  time1 = parser.parse(t1)
  return (time2 - time1).total_seconds()


def compute_speed(d1, d2, t1, t2):
    """
    t1 = datetime.strptime(time.strftime('%H:%M:%S',time.gmtime(float(t1)/1000.)),
     '%H:%M:%S')
    t2 = datetime.strptime(time.strftime('%H:%M:%S',time.gmtime(float(t2)/1000.)),
     '%H:%M:%S')
    #duration = (t2 - t1).total_seconds()
    """
 
    duration = duration_between_two_calls(time.strftime('%H:%M:%S',time.gmtime(float(t1)/1000.)),
     time.strftime('%H:%M:%S',time.gmtime(float(t2)/1000.)))
    distance = d2 - d1

    return round((distance / duration), 2)  

def map_lineIDs(lines):
    return {'lineId':lines['lineId'],'SpeedLineId':[],'TimelineId':[]}
def map_lines(responses):
    li = [item  for res in responses for item in res['lines']]
    return list(map(map_lineIDs, li))


def load_json_file(json_file): 
  with open(json_file, "r") as read_file:
    data = json.load(read_file)
  return data

  return list_track

"""
files = [file for file in glob.glob("./data/*.json")]
for file_name in files:
    with io.open(file_name, 'rb') as image_file:
        content = image_file.read()
"""

data = load_json_file("./splited_json/splited_json/vehiclePosition01.json")
testlist = data['data'][0:]
f = open("./demofile2.txt", "a")
f.write(str(testlist))
f.close()

print("this is 2 calls of data \n",data['data'][0]['Responses'] )
lineIDs_first_lines = data['data'][1]['Responses'][0]['lines']
print("this is different lineIDs in first lines response \n\n", lineIDs_first_lines,"\n\n")
nb_vehicle_positions = len(lineIDs_first_lines[0]['vehiclePositions'])
print("this is how many vehicle we hve in line 1, first call`\n",nb_vehicle_positions)
speed_list = map_lines(testlist[0]['Responses']) # Prepare a list of lineID with it list of speed over timestamps calls    
#li_time = list(map(map_time, testlist))# Map the timestamp list
for idx, call in enumerate(testlist): # loop in all timestamp responses
    time_call = call['time']# get the timestamp
    api_lines_responses = [item for resp in
     call['Responses'] if resp is not None for item in resp['lines']]
      #Since the API calls is recorded sequentualy, map all lineIDs within all lines responses in one list with skipping None responses
    for line in api_lines_responses:
        # get vehicle position list for each line
        speed_stop_list = [] #at the end of the loop, this list contain all speed in all stop station
        speed_first_api_call = []
        if idx == 0 : # case when we are in the first timestamp, compute the speed = distanceFromPointID/ 30 secondes
            speed_first_api_call = [pos['distanceFromPoint']/30.00 for pos in
             line['vehiclePositions']] 
        else: # general cases when we are in Timestamp Ti
            last_time_call = testlist[idx-1]['time'] # get the last timestamp
            for vehicle_position in line['vehiclePositions']: # loop on all vehicle position per lineID
                map_last_distance = [pos for resp in testlist[idx-1]['Responses']
                 if resp is not None for item in resp['lines'] for pos in
                  item['vehiclePositions']] # this map the last timestamp distances for all pointID 
                for di in map_last_distance: # loop on all last distance in order to compare direction, pointID, and distance i should be >distance i-1
                    if vehicle_position['directionId'] == di['directionId'] and \
                     vehicle_position['pointId'] == di['pointId'] and \
                      vehicle_position['distanceFromPoint'] >= di['distanceFromPoint']:  
                        # thischeck get the most relevant data to compute speed
                        speed = compute_speed(di['distanceFromPoint'],
                         vehicle_position['distanceFromPoint'], last_time_call,
                          time_call)
                        #speed = (vehicle_position['distanceFromPoint'] -
                        # di['distanceFromPoint'])/(int(time_call) - int(last_time_call)) 
                        #print("\n and this is the speed" , speed)
                        speed_stop_list.append(speed) # add new stop speed
                        break

        for li in speed_list:# loop in the prepared list of speed per line and update it with new speed
                if li['lineId'] == line['lineId']:
                    if idx == 0 and speed_first_api_call:
                        li['SpeedLineId'].append(statistics.mean(speed_first_api_call))## get the reel stat mean after!!
                        li['TimelineId'].append(time.strftime('%H:%M:%S',
                         time.gmtime(float(time_call)/1000.)))
                    elif  speed_stop_list:
                        li['SpeedLineId'].append(statistics.mean(speed_stop_list))
                        li['TimelineId'].append(time.strftime('%H:%M:%S',
                         time.gmtime(float(time_call)/1000.)))
                    break

# write to file
"""
f = open("./demofile2.txt", "a")
f.write(str(speed_list))
f.close()
"""
# Ploting
"""
xaxis  = speed_list[2]['TimelineId'][0::120]
yaxis =  speed_list[2]['SpeedLineId'][0::120]
plt.title("LineID ="+speed_list[2]['lineId'])
plt.plot(xaxis, yaxis)
plt.xlabel('time (s)')
plt.ylabel('Speed (m/s)')
plt.show()
"""