import json
import time
from datetime import datetime, timedelta
import numpy as np

import matplotlib.pyplot as plt



"""
.Data preparation steps
    - time is an eposh type >>>> cast to timestamp
    - data is organized in json file convert to python
    - the data is it ordred or not?
"""
# read json file

# getting 2 timestamp (2 calls)
with open("./splited_json/splited_json/vehiclePosition13.json", "r") as read_file:
    data = json.load(read_file)
testlist = data['data'][0:8]

for obj in testlist:
    obj['time'] = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(float(obj['time'])/1000.))  
#print(testlist)
time_axis = [obj['time'] for obj in testlist]


ti = [datetime.strptime(date, "%Y-%m-%d %H:%M:%S") for date in time_axis] # str time converted to datetime type

t0 = ti[0] # initial time

timespoint = [i - t0 for i in ti] # differents Time point (axis time)
#print(ti,'\n',t0 ,'\n', timespoint )

distance_line1 = testlist[0]['Responses'][0]['lines'][0]
distance = [obj['Responses'][0]['lines'][0]['vehiclePositions'] for obj in testlist]
print(distance)
speedavg_line1 = []

"""
# Ploting

speed= range(7,10)
print(t0,t1,t2)
plt.plot(times,speed)
plt.show()
"""