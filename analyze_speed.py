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

#df = pd.read_json("./splited_json/splited_json/vehiclePosition13.json")
#df_data = df['data'][0:3]
testlist = data['data'][0:4]
li_time = list(map(map_time, testlist))

def map_lineIDs(lines):
    return {'lineId':lines['lineId'],'SpeedLineId':[]}
def map_lines(responses):
    li = [item  for res in responses for item in res['lines']]

    return list(map(map_lineIDs, li)) 
#responses = list(map(map_responses, testlist))
#print(responses)

speed_list = map_lines(testlist[0]['Responses'])    
print(speed_list)
for idx, call in enumerate(testlist) :
    time_call = call['time']
    #print(call['Responses'])
    api_lines_responses = [item  for resp in call['Responses']  for item in resp['lines']]
    #print("###########new API call arrive#### "+str(idx)+"\n")
    for line in api_lines_responses:
         # get vehicle position list for each line
        
        if idx == 0 :
            speed_first_distance = [pos['distanceFromPoint']/30 for pos in line['vehiclePositions']] 
            print(speed_first_distance)

            #print("first list speed per line \n",speed_per_line)
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
                        #print('relevant data after checking \n',speed_per_line)
                        break
    
                #print("this is the last map distance\n",map_last_distance)

        for li in speed_list:
                if li['lineId'] == line['lineId']:
                    if idx == 0 :
                        li['SpeedLineId'].append(0.0)## get the reel stat mean after!!
                    else:
                        li['SpeedLineId'].append(statistics.mean(speed_stop_list))
                    break
                    #speed_per_line.append(statistics.mean(speed_per_line))
print(">>>>>>>>this is the final \n",speed_list)
            
    # case when we are in the first tipe point >>> idx = 0
    


    

#lines = list(map(map_lines, responses))
#print(responses)

"""

 for line in lines:
    if pointId in map list before 
        then >>>  speed = di - di-1/ ti-ti-1
    else : speed = 0        
"""
"""
position_list_line1 = []
for obj in testlist:
    for resp in obj['Responses']:
            for line in resp['lines']:
                if line['lineId'] == '1' :
                    position_list_line1.append(line['vehiclePositions']) 
                #res = list(map(list, (line ['vehiclePositions'][0:-1])))
                #print("call object lines",res)
"""
#lst = oneway_direction(position_list_line1 , '8731')
#lst = extract_only_same_pointId(lst)
#print("This is the list of vehiclePositions LineId = 1 in 2 times for one direction 8731\n" , position_list_line1)

## convert to timestamp
#for obj in testlist:
#    obj['time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(float(obj['time'])/1000.))  
#print(testlist)
#time_axis = [obj['time'] for obj in testlist]
####

# str time converted to datetime type
#ti = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S") for date in time_axis] 
####



# differents Time point (axis time)
#timespoint = [i - t0 for i in ti] 
###
#print(ti,'\n', testlist[0:2])

distance_line1 = testlist[0]['Responses'][0]['lines'][0]
#distance = [obj['Responses'][0]['lines'][0]['vehiclePositions'] for obj in testlist]
#print(distance)
speedavg_line1 = []

"""
# Ploting

speed= range(7,10)
print(t0,t1,t2)
plt.plot(times,speed)
plt.show()







  
# initializing dictionary
test_dict = {'gfg' : [4, 6, 7, 8],
             'is' : [3, 8, 4, 2, 1],
             'best' : [9, 5, 2, 1, 0]}
  
# printing original dictionary
print("The original dictionary is : " + str(test_dict))
  
# Extracting Dictionary values to List
# Using map()
res = list(map(list, (test_dict.values())))
"""