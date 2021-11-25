import csv
from datetime import datetime, timedelta
import os
import ijson
from geopy.distance import geodesic
import pandas as pd
import time
from data_preparation.offline_speed import OfflineSpeed


class DataLoader:
    stop_time_file_name = ""  # stop times file name
    stop_coords_file_name = ""  # stop file name
    vehicle_position_files = []
    vehicle_position_folder = ""
    stop_coords = {}
    simplified_vehicle_position_file = os.path.join(os.getcwd(), "data_preparation", "generated_files",
                                                    "vehicle_position.txt")
    online_offline_data_file = os.path.join(os.getcwd(), "data_preparation", "generated_files",
                                            "online_offline_data.txt")
    vehicle_positions = {}
    offline_speed = pd.DataFrame()

    def __init__(self, stop_time_file_name, stop_coords_file_name, vehicle_position_folder):
        self.stop_time_file_name = stop_time_file_name
        self.stop_coords_file_name = stop_coords_file_name
        self.vehicle_position_folder = vehicle_position_folder
        self.vehicle_position_files = os.listdir(vehicle_position_folder)
        # initialize the hash table keys
        for i in range(1, 100):
            index = str(i)
            self.vehicle_positions[index] = {}

    def simplify_data_shape(self):
        # in case where the file is already generated and full of data
        file_empty = os.stat(self.simplified_vehicle_position_file).st_size < 4
        if file_empty is False: return

        current_file = os.path.join(self.vehicle_position_folder, self.vehicle_position_files[0])
        file = open(self.simplified_vehicle_position_file, 'w')
        writer = csv.writer(file)
        writer.writerow(['line_id', 'directionId', 'pointId', 'distanceFromPoint', 'time'])
        with open(current_file) as file:
            objects = ijson.items(file, 'data.item')
            for data in objects:
                for response in data["Responses"]:
                    try:
                        for line in response["lines"]:
                            for position in line["vehiclePositions"]:
                                row_data = [line["lineId"],
                                            position["directionId"],
                                            position["pointId"],
                                            position['distanceFromPoint'],
                                            data['time']]
                                writer.writerow(row_data)
                    except TypeError:
                        continue
        file.close()

    def extract_offline_online_data(self):
        """
        After the generation of a simplified shape for the vehicle position
        it needs to be used to actually compute the realtime speed, but first it
        needs to be mapped to a dictionary to ease up the access
        :return:
        """
        # check initially if the data already in the file
        file_empty = os.stat(self.online_offline_data_file).st_size < 4
        if file_empty is False: return

        self.simplify_data_shape()
        file = open(self.online_offline_data_file, 'w')
        csv_writer = csv.writer(file)
        csv_writer.writerow(['actual_time', 'expected_time', 'speed', 'line_id'])
        dataframe = pd.read_csv(self.simplified_vehicle_position_file)
        data = dataframe.groupby(['line_id'])
        for state, frame in data:
            line_id = str(frame['line_id'].values[0])
            grouped_frame = frame.groupby('time')
            for state2, dframe in grouped_frame:
                self.__to_vehicle_position_dict(dframe, line_id)
        self.__extract_row_data(csv_writer)
        self.vehicle_positions.clear()

    def __to_vehicle_position_dict(self, dframe, line_id):
        """"
        receives a Dataframe and a line-id to append different vehicle positions to a specific line_id
        which also correspond to the dict key
        """
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

    def load_stops(self):
        with open(self.stop_coords_file_name) as file:
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
                    self.stop_coords[first_digits][stop] = row  # insert elements at the right index
                except KeyError:
                    self.stop_coords[first_digits] = {}
                    self.stop_coords[first_digits][stop] = row
                    continue

    def __extract_row_data(self, csv_writer):
        for index1 in self.vehicle_positions:
            lines = self.vehicle_positions[index1]
            for index2 in lines:
                data = lines[index2]['data']
                length = len(data)
                for i in range(1, length):
                    item1 = data[i - 1]
                    item2 = data[i]
                    try:
                        distance = self.__calculate_distance(point1=str(item1['pointId']),
                                                             point2=str(item2['pointId']),
                                                             distance_point1=item1['distanceFromPoint'],
                                                             distance_point2=item2['distanceFromPoint'])
                        speed, time1, time2 = self.__calculate_speed(distance, item1['time'], item2['time'], True)
                        if speed == 0: continue  # exclude the speed
                        expected_arrival_time = self.__calculate__offline_time(item1['time'], item2['pointId'],
                                                                               distance)
                        csv_writer.writerow(
                            [time2.strftime("%H:%M:%S"), expected_arrival_time, speed, item2['line_id']])
                    except:
                        continue

    def __calculate_distance(self, point1, point2, distance_point1=0, distance_point2=0, ):
        try:
            first_digits = point1[:2]
            info_point_id = self.stop_coords[first_digits][point1]
            departure_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            first_digits = point2[:2]
            info_point_id = self.stop_coords[first_digits][point2]
            arrival_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            distance = geodesic(departure_coord, arrival_coord).meters - distance_point1 + distance_point2
            return distance
        except KeyError:
            return

    @staticmethod
    def __calculate_speed(distance, time1, time2, is_epoch_time=False):
        if is_epoch_time:
            time1 = time.strftime('%H:%M:%S', time.localtime(int(str(time1)[:10])))
            time2 = time.strftime('%H:%M:%S', time.localtime(int(str(time2)[:10])))
        time1 = datetime.strptime(time1, '%H:%M:%S')
        time2 = datetime.strptime(time2, '%H:%M:%S')
        time_diff = (time2 - time1).total_seconds()
        if distance is None:
            return None
        else:
            return (distance / time_diff), time1, time2

    def set_offline_speed(self):
        offline_speed = OfflineSpeed(stop_time_file_name=self.stop_time_file_name,
                                     stop_coords=self.stop_coords,
                                     compute_speed_func=self.__calculate_speed,
                                     compute_distance_func=self.__calculate_distance)

        file = offline_speed.container_file
        file_empty = os.stat(file).st_size == 0
        if not file_empty:
            self.offline_speed = pd.read_csv(file)

        else:
            offline_speed.generate_file()

    def __calculate__offline_time(self, departure_time, point_id, distance):
        speed = self.offline_speed.loc[self.offline_speed['point_id'] == str(point_id)]['speed'].values[0]
        speed = float(speed)
        expected_time = distance / speed
        expected_time = timedelta(seconds=expected_time)
        departure_time = time.strftime('%H:%M:%S', time.localtime(int(str(departure_time)[:10])))
        departure_time = datetime.strptime(departure_time, '%H:%M:%S')
        return (departure_time + expected_time).strftime("%H:%M:%S")
