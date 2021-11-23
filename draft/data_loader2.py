import csv
import math
import os
from datetime import timedelta
import ijson
from geopy.distance import geodesic
import pandas as pd


class DataLoader:
    stop_times_fname = ""  # stop times file name
    stops_fname = ""  # stop  file name
    vehicle_position_files = []
    vehicle_position_folder = ""
    line_ids = {}
    stops = {}
    file_writer = os.path.join(os.getcwd(), "generated_file", "line_speed.txt")

    def __init__(self, stop_times_file, stops_file, vehicle_position_folder):
        self.stop_times_fname = stop_times_file
        self.stops_fname = stops_file
        self.vehicle_position_folder = vehicle_position_folder
        self.vehicle_position_files = os.listdir(vehicle_position_folder)
        # initialize the hash table keys
        for i in range(10, 100):
            index = str(i)
            self.line_ids[index] = {}

    def load_stops(self):
        with open(self.stops_fname) as file:
            csv_reader = csv.DictReader(file, delimiter=',')
            for row in csv_reader:
                stop = str(row["stop_id"])
                first_digits = stop[:2]
                # removing the useless attribute, to save some memory
                if "stop_code" in row: row.pop("stop_code")
                if "stop_desc" in row: row.pop("stop_desc")
                if "zone_id" in row: row.pop("zone_id")
                if "stop_url" in row: row.pop("stop_url")
                if "location_type" in row: row.pop("location_type")
                # clearing some extra space
                row['stop_lat'] = float(str(row['stop_lat']).strip())
                row['stop_lon'] = float(str(row['stop_lon']).strip())
                try:
                    self.stops[first_digits][stop] = row  # insert elements at the right index
                except KeyError:
                    self.stops[first_digits] = {}
                    self.stops[first_digits][stop] = row
                    continue

    def load_stop_times(self):
        attributes = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type",
                      "drop_off_type"]
        data_frame = pd.read_csv(
            self.stop_times_fname,
            usecols=attributes,
            parse_dates=["arrival_time", "departure_time"])
        data = data_frame.groupby(["trip_id"])
        for state, frame in data:
            data_array = frame.to_dict('records')
            length = len(data_array)
            line_id = math.nan
            dictionary = []
            for i in range(1, length):
                data1 = data_array[i - 1]
                data2 = data_array[i]
                distance = self.__compute_distance(data1['stop_id'], data2['stop_id'])
                speed = self.__compute_speed(distance, data1['departure_time'], data2['arrival_time'])
                if line_id is math.nan:
                    line_id = self.__get_line_id(data2['stop_id'])

                element = {'line_id': line_id, 'speed': speed, 'time': data2['arrival_time']}
                dictionary.append(element)

            subDataFrame = pd.DataFrame(dictionary)
            subDataFrame['line_id'] = subDataFrame['line_id'].fillna(line_id)
            # write it to file
            subDataFrame.to_csv(path_or_buf=self.file_writer, index=False, header=False, mode='a')

    def __compute_distance(self, departure_id, arrival_id, ):
        try:
            first_digits = departure_id[:2]
            info_point_id = self.stops[first_digits][departure_id]
            departure_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            first_digits = arrival_id[:2]
            info_point_id = self.stops[first_digits][arrival_id]
            arrival_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            distance = geodesic(departure_coord, arrival_coord).meters
            return distance
        except KeyError:
            return

    @staticmethod
    def __compute_speed(distance, departure_time, arrival_time):
        hours, minutes, seconds = map(int, departure_time.split(':'))
        departure_time = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        hours, minutes, seconds = map(int, arrival_time.split(':'))
        arrival_time = timedelta(hours=hours, minutes=minutes, seconds=seconds)
        t = (arrival_time - departure_time).total_seconds()
        speed = distance / t
        return "{:.2}f".format(speed)

    def __get_line_id(self, point_id):
        try:
            first_digits = point_id[:2]
            line_id = self.line_ids[first_digits][point_id]
            return line_id
        except KeyError:
            return math.nan

    def set_point_line_id(self):
        current_file = os.path.join(self.vehicle_position_folder, self.vehicle_position_files[0])
        with open(current_file) as file:
            objects = ijson.items(file, 'data.item')
            for data in objects:
                for response in data["Responses"]:
                    try:
                        for line in response["lines"]:
                            line_id = line["lineId"]
                            for position in line["vehiclePositions"]:
                                point_id = position["directionId"]
                                first_digits = point_id[:2]
                                self.line_ids[first_digits][point_id] = line_id

                                point_id = position["pointId"]
                                first_digits = point_id[:2]
                                self.line_ids[first_digits][point_id] = line_id

                    except TypeError:
                        continue
