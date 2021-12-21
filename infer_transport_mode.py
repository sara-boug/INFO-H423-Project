import json
import time
from datetime import datetime, timedelta
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
from numpy import genfromtxt
import pandas as pd
from dateutil import parser
from geopy.distance import great_circle
import statistics
import csv


######################INFER THE MODE OF TRANSPORT FROM A GIVEN TRACK#########

def map_track_id(obj):
	return obj['trackid']

def check_exist_trackid(list_track, trackid):

	li = list(map(map_track_id,list_track))
	if trackid in li:
		return True
	else: 
		return False

def update_track_list(list_track, track):
	obj = next(item for item in list_track if item["trackid"] == track[0])
	distance = distance_between_two_point(track[1], track[2],
	 obj['positions'][-1][0], obj['positions'][-1][1])
	duration = duration_between_two_calls(obj['time'][-1], track[3])
	acceleration = round((distance / duration)* 3.6, 2)  
	obj['velocity'].append(acceleration)
	obj['distance'].append(distance)
	obj['time_to_plot'].append(str(parser.parse(track[3]).time()))
	obj['positions'].append((track[1],track[2]))
	obj['time'].append(track[3])
	return list_track



def duration_between_two_calls(t1, t2):
	time2 = parser.parse(t2)
	time1 = parser.parse(t1)
	return (time2 - time1).total_seconds()

def distance_between_two_point(lat_pt1, long_pt1, lat_pt2, long_pt2):
	coord_pt1 = (float(lat_pt1), float(long_pt1))
	coord_pt2 = (float(lat_pt2), float(long_pt2))

	return great_circle(coord_pt1, coord_pt2).m



def map_csvtrack(csv_file):	
	df=pd.read_csv(csv_file, sep=',', header=None)
	list_track = [{'trackid':'0', 'positions':[],'time':[], 'velocity':[],'distance':[] },]
	list_mean =[]
	f = open('./result_mean.txt', "a")
	f.write('trackID'+','+'mean_velocity'+','+'velocities'+'\n')

	for item in df.values[1:]:
		if check_exist_trackid(list_track, item[0]):
			update_track_list(list_track, item)
		else:
			obj = {'trackid':item[0],'mean': 0,
			 'positions':[(item[1], item[2])], 'time':[item[3]], 'distance':[], 'velocity':[],
			  'time_to_plot':[] }
			list_track.append(obj)
	for res in list_track[1:]:
		res['mean'] = statistics.mean(res['velocity'])
		list_mean.append((res['trackid'],res['mean']))
		f.write(str(str(res['trackid'])+','+str(res['mean'])+','+str(res['velocity'])+'\n'))
	
	f.close()
	return list_track, list_mean

def mean_speed_onlinedata(csv_file):
	mean_speeds =[]
	with open(csv_file, "r") as f:
		reader = csv.reader(f)
		for  i,line in enumerate(reader):
			if line[4:][0] != "None":
				speed = list(line[5:])
				speed[-1] = speed[-1][:-1]
				new_speed = list(map(float, speed))
				mean_speeds.append(statistics.mean(new_speed))
	return mean_speeds

accel_list,list_mean = map_csvtrack('./GPStracks.csv')
#online_speed = mean_speed_onlinedata('./online_speeds/speeds.txt')

# write to file

f = open("./result_acceleration.txt", "a")
f.write(str(accel_list))
f.close()