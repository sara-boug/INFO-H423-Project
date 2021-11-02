import csv
import logging
import traceback
import os
import time
import json
from datetime import datetime, timedelta


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
                            "latitude": stop_csv["stop_lat"],
                            "longitude": stop_csv["stop_lon"],
                        }
                        current_element[stop_id] = stop_obj
                    # in case where the object is already initiated then in this case, just append to the array
                    else:
                        stop_obj = current_element.get(stop_id)
                        stop_obj["info"].append(row)
                        current_element[stop_id] = stop_obj
        except Exception:
            logging.log(traceback.format_exc())

    def computeSpeed(self):
        current_file = os.path.join(self.vehicle_position_folder, self.vehicle_position_files[2])
        with open(current_file) as file:
            json_reader = json.load(file)
            for data in json_reader["data"]:
                epoch_time = int(data["time"][:10])
                data_retrieval_time = time.strftime('%H:%M:%S', time.localtime(epoch_time))
                for response in data["Responses"]:
                    for line in response["lines"]:
                        line_id = line["lineId"]
                        for position in line["vehiclePositions"]:
                            self.__findCorrectRowData(position["directionId"],
                                                      position["pointId"],
                                                      data_retrieval_time)
                            return

    def __findCorrectRowData(self, direction_id, point_id, data_retrieval_time):
        first_digits = direction_id[:2]
        # accessing the elements  in the second layer of the stops time
        direction = self.stop_times[first_digits][direction_id]
        # for the direction id we need to find the nearest time after the data retrieval
        direction_info = direction["info"]
        new_min = None
        needed_info = self.__findMinDate(direction_info, data_retrieval_time)
        first_digits = point_id[:2]
        point = self.stop_times[first_digits][point_id]  # accessing the elements  in the second layer of the stops time
        point_info = point["info"]
        needed_info = self.__findMinDate(point_info, data_retrieval_time)
        print(needed_info)

        # Now the stop we are interested in is the stop that occurred after the retrieval time

    def __findMinDate(self, stop_info, data_retrieval_time):
        new_min = None
        needed_info = None
        threshold = datetime.strptime(str('00:00:00').strip(), '%H:%M:%S') - datetime.strptime(str('00:00:00').strip(),
                                                                                               '%H:%M:%S')
        for info in stop_info:
            print(info)
            try:
                min_ = new_min
                departure_time = datetime.strptime(str(info["departure_time"]).strip(), '%H:%M:%S')
                retrieval_time = datetime.strptime(str(data_retrieval_time).strip(), '%H:%M:%S')
                new_min = retrieval_time - departure_time
                print(new_min)
                dep_time = time.strptime(str(info["departure_time"]).strip(), '%H:%M:%S')
                range_time = dep_time + datatimedelta(hours=1, minutes=30)
                ret_time = time.strptime(str(data_retrieval_time).strip(), '%H:%M:%S')

                if min_ is not None and range_time > dep_time > ret_time:
                    if min_ < new_min:
                        new_min = min_
                        needed_info = info
                        continue
            except ValueError:
                continue
        print(needed_info)
        return needed_info
