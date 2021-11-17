import csv
import math
import os
import ijson
from geopy.distance import geodesic
import pandas as pd
import time


class DataLoader:
    stop_times_fname = ""  # stop times file name
    stops_fname = ""  # stop  file name
    vehicle_position_files = []
    vehicle_position_folder = ""
    stops = {}
    simplified_vehicle_position_file = os.path.join(os.getcwd(), "generated_file", "vehicle_position.txt")
    vehicle_positions = {}

    def __init__(self, stop_times_file, stops_file, vehicle_position_folder):
        self.stop_times_fname = stop_times_file
        self.stops_fname = stops_file
        self.vehicle_position_folder = vehicle_position_folder
        self.vehicle_position_files = os.listdir(vehicle_position_folder)
        # initialize the hash table keys
        for i in range(1, 100):
            index = str(i)
            self.vehicle_positions[index] = {}

    def simplify_shape(self):
        current_file = os.path.join(self.vehicle_position_folder, self.vehicle_position_files[0])
        file = open(self.simplified_vehicle_position_file, 'w')
        writer = csv.writer(file)
        writer.writerow(['line_id', 'directionId', 'pointId', 'time'])
        with open(current_file) as file:
            objects = ijson.items(file, 'data.item')
            for data in objects:
                t = time.strftime('%H:%M:%S', time.localtime(int(data['time'][:10])))
                for response in data["Responses"]:
                    try:
                        for line in response["lines"]:
                            for position in line["vehiclePositions"]:
                                row_data = [line["lineId"],
                                            position["directionId"],
                                            position["pointId"],
                                            position['distanceFromPoint'],
                                            t]
                                writer.writerow(row_data)
                    except TypeError:
                        continue
        file.close()

    def analyse_data(self):
        dataframe = pd.read_csv(self.simplified_vehicle_position_file)
        data = dataframe.groupby(['line_id'])
        counter = 0
        for state, frame in data:
            try:
                line_id = str(frame['line_id'][0])
                grouped_frame = frame.groupby('time')
                assign = False
                for state2, dframe in grouped_frame:
                    self.__to_vehicle_position_dict(dframe, line_id)

            except KeyError:
                continue

        print(self.vehicle_positions['1']['2'])

    def __to_vehicle_position_dict(self, dframe, line_id):
        dict_ = dframe.to_dict('records')
        counter = 0
        for element in dict_:
            try:
                data_array = self.vehicle_positions[line_id][str(counter)]['data']
                data_array = data_array + [element]
                self.vehicle_positions[line_id][str(counter)]['data'] = data_array
            except KeyError:
                self.vehicle_positions[line_id][str(counter)] = {
                    'data': [element]
                }
            counter += 1

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
