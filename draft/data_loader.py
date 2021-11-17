import csv
import logging
import traceback
import os
import time
from datetime import datetime, timedelta
import ijson
from geopy.distance import geodesic
import pandas as pd
import math


class DataLoader:
    stop_times_fname = ""  # stop times file name
    stops_fname = ""  # stop  file name
    vehicle_position_files = []
    vehicle_position_folder = ""
    stop_times = {}
    stops = {}

    def __init__(self, stop_times_file, stops_file, vehicle_position_folder):
        self.stop_times_fname = stop_times_file
        self.stops_fname = stops_file
        self.vehicle_position_folder = vehicle_position_folder
        self.vehicle_position_files = os.listdir(vehicle_position_folder)
        # initialize the hash table keys
        for i in range(10, 100):
            index = str(i)
            self.stop_times[index] = {}

    def load_stops(self):
        try:
            with open(self.stops_fname) as file:
                csv_reader = csv.DictReader(file, delimiter=',')
                for row in csv_reader:
                    stop = str(row["stop_id"])
                    # removing the useless attribute, to save some memory
                    if "stop_code" in row: row.pop("stop_code")
                    if "stop_desc" in row: row.pop("stop_desc")
                    if "zone_id" in row: row.pop("zone_id")
                    if "stop_url" in row: row.pop("stop_url")
                    if "location_type" in row: row.pop("location_type")
                    # clearing some extra space
                    row['stop_lat'] = str(row['stop_lat']).strip()
                    row['stop_lon'] = str(row['stop_lon']).strip()
                    self.stops[stop] = row  # insert elements at the right index
        except Exception:
            logging.log(traceback.format_exc())

    def load_stop_times(self):
        try:
            with open(self.stop_times_fname) as file:
                current_element = None
                csv_reader = csv.DictReader(file, delimiter=',')
                for row in csv_reader:
                    stop_id = str(row["stop_id"])
                    # initially useless attributes should be omitted
                    if "trip_id" in row: row.pop("trip_id")
                    if "pickup_type" in row: row.pop("pickup_type")
                    if "drop_off_type" in row: row.pop("drop_off_type")
                    try:
                        row['departure_time'] = datetime.strptime(row['departure_time'].strip(), '%H:%M:%S')
                        row['arrival_time'] = datetime.strptime(row['arrival_time'].strip(), '%H:%M:%S')
                    except ValueError:
                        continue
                    # stop id is omitted as it is no longer needed
                    # if "stop_id" in row: row.pop("stop_id")
                    first_digits = stop_id[0:2]  # we need the first digits for the the dictionary keys
                    try:
                        current_element = self.stop_times[first_digits]
                    except KeyError:
                        self.stop_times[first_digits] = {}
                        current_element = self.stop_times[first_digits]

                    # if the key doesn't exist then in this case the stop elements does not exist yet
                    if stop_id not in current_element:
                        stop_csv = self.stops[stop_id]  # extracting the object having the intended long
                        stop_obj = {
                            "info": [row],
                            "latitude": float(stop_csv["stop_lat"]),
                            "longitude": float(stop_csv["stop_lon"]),
                        }
                        current_element[stop_id] = stop_obj
                    # in case where the object is already initiated then in this case, just append to the array
                    else:
                        stop_obj = current_element.get(stop_id)
                        stop_obj["info"].append(row)
                        current_element[stop_id] = stop_obj
        except Exception:
            logging.log(level=1, msg=traceback.format_exc())

    def sort_hash_table(self):
        data_retrieval_time = time.strftime('%H:%M:%S', time.localtime(1631615671))
        data_retrieval_time = datetime.strptime(data_retrieval_time, '%H:%M:%S')
        for key1 in self.stop_times:
            for key2 in self.stop_times[key1]:
                layer2 = self.stop_times[key1][key2]
                layer2["info"].sort(key=lambda x: x["departure_time"])
                departure, arrival = self.__findMinDate(layer2["info"], data_retrieval_time)
                self.stop_times[key1][key2]["departure"] = departure
                self.stop_times[key1][key2]["arrival"] = arrival
                self.stop_times[key1][key2]["info"] = []

    def computeSpeed(self):
        current_file = os.path.join(self.vehicle_position_folder, self.vehicle_position_files[0])
        data_loading_file = os.path.join(os.getcwd(), "generated_file", "line_speed.txt")
        header = ["line_id", "speed"]
        with open(data_loading_file, "w") as loading_file:
            writer = csv.writer(loading_file)
            writer.writerow(header)
            with open(current_file) as file:
                objects = ijson.items(file, 'data.item')
                for data in objects:
                    for response in data["Responses"]:
                        try:
                            for line in response["lines"]:
                                line_id = line["lineId"]
                                for position in line["vehiclePositions"]:
                                    speed = self.computeSpeed_(position["directionId"],
                                                               position["pointId"], position["distanceFromPoint"])
                                    output_data = [int(line_id), speed]
                                    writer.writerow(output_data)
                        except TypeError:
                            continue
                        continue

    def computeSpeed_(self, direction_id, point_id, distance_from_point):
        try:
            first_digits = point_id[:2]
            info_point_id = self.stop_times[first_digits][point_id]
            departure = info_point_id['departure']
            coord1 = (info_point_id['latitude'], info_point_id['longitude'])
            first_digits = direction_id[:2]
            info_direction_id = self.stop_times[first_digits][direction_id]
            arrival = info_direction_id['arrival']
            coord2 = (info_direction_id['latitude'], info_direction_id['longitude'])
            distance = geodesic(coord1, coord2).meters - float(distance_from_point)
            t = (arrival - departure).total_seconds()
            speed = distance / t
            if math.isnan(speed): return
            else :
                return "{:.3}f".format(speed)
        except KeyError:
            return
        # Now the stop we are interested in is the stop that occurred after the retrieval time

    @staticmethod
    def __findMinDate(stop_info, data_retrieval_time):
        info_data_frame = pd.DataFrame(stop_info)
        time_range = timedelta(hours=1, minutes=30)
        departure_range = (data_retrieval_time - time_range)
        arrival_range = (data_retrieval_time + time_range)
        departure_mask = (info_data_frame['departure_time'] > departure_range) & \
                         (info_data_frame['departure_time'] < data_retrieval_time)
        arrival_mask = (info_data_frame['arrival_time'] < arrival_range) & \
                       (info_data_frame['arrival_time'] > data_retrieval_time)
        min_departure = (info_data_frame.loc[departure_mask])['departure_time'].min()
        min_arrival = (info_data_frame.loc[arrival_mask])['arrival_time'].min()
        return min_departure, min_arrival
