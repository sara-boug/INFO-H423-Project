import csv
import os
import time

from datetime import datetime, timedelta
import ijson
from geopy.distance import geodesic
import pandas as pd

from data_preparation.offline_speed import OfflineSpeed


class DataLoader:
    stop_time_file_name = ""  # stop times file name
    stop_coords_file_name = ""  # stop file name
    vehicle_position_files = []
    vehicle_position_folder = ""
    stop_coords = {}
    simplified_vehicle_position_file = os.path.join(os.getcwd(), "data_preparation", "generated_files",
                                                    "vehicle_position.txt")

    vehicle_positions = {}
    offline_speed = pd.DataFrame()

    def __init__(self, stop_time_file_name, stop_coords_file_name, vehicle_position_file, online_offline_data_file):
        self.stop_time_file_name = stop_time_file_name
        self.stop_coords_file_name = stop_coords_file_name
        self.vehicle_position_file = vehicle_position_file
        self.online_offline_data_file = online_offline_data_file
        # initialize the sparse tree
        for i in range(1, 100):
            index = str(i)
            self.vehicle_positions[index] = {}

    def simplify_data_shape(self):
        """
        Simplifies the vehicle position file and write it to a csv file

        The csv file has this format :
        line_id,directionId,pointId,distanceFromPoint,time
        1,8161,8012,0,1631177627260
        1,8162,8162,0,1631177627260
        1,8161,8733,0,1631177627260

        This allows manipulating the data easier and faster
        """

        current_file = self.vehicle_position_file
        file = open(self.simplified_vehicle_position_file, 'w')
        writer = csv.writer(file)

        # setting the rows
        writer.writerow(['line_id', 'directionId', 'pointId', 'distanceFromPoint', 'time'])

        # generate the necessary data
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
        self.simplify_data_shape()
        file = open(self.online_offline_data_file, 'w')
        csv_writer = csv.writer(file)
        csv_writer.writerow(['actual_time', 'expected_time', 'speed', 'line_id'])
        dataframe = pd.read_csv(self.simplified_vehicle_position_file)
        # grouping per line_id
        data = dataframe.groupby(['line_id'])

        # looping through this grouped data
        for state, frame in data:
            # extract the line_id
            line_id = str(frame['line_id'].values[0])
            # we group further by time
            grouped_frame = frame.groupby('time')
            for state2, dframe in grouped_frame:
                # loop through thus grouped data and pass it to this function to allow generating the necessary rows
                self.__to_vehicle_position_dict(dframe, line_id)

        # when the frame is loaded
        self.__extract_row_data(csv_writer)
        self.vehicle_positions.clear()
        file.close()

    def __to_vehicle_position_dict(self, dframe, line_id):
        """"
        receives a Dataframe and a line-id, then generate a dictionary having the line_id key
        as each chunk of data is ordered, a counter is set to keep track of the exact position of the data

        hence, this dictionary is characterized by two layers
        { 'line_id-num':
           { 'position_count-num':
              { 'data' : [{
                    'line_id':..., 'directionId':..., 'pointId':..., 'distanceFromPoint':..., 'time':...
                    }]
             }
           }
        }
        """
        dict_ = dframe.to_dict('records')
        counter = 0
        for element in dict_:
            try:
                # update the dictionary data
                data_array = self.vehicle_positions[line_id][str(counter)]['data']
                data_array = data_array + [element]
                self.vehicle_positions[line_id][str(counter)]['data'] = data_array
            except KeyError:
                # in case where the key does not exist yet , then it should be initialised
                self.vehicle_positions[line_id][str(counter)] = {
                    'data': [element]
                }
            counter += 1

    def load_stops(self):
        """
        Transforms the stops coordinates into a suitable format, to increase the access efficiency

        As number of stops is somehow numerous, iterating over allover the data might be time consuming,
        hence, an efficient way is to organize the whole data into a sparse tree, it is more a like a dictionary
        with suitable layers ,

         for instance a point id 8081 will be located as follow
        {
            '80': {
               '8081' : {'stop_lat':...,'stop_lon':..... }
            }
        }
        Hence , to find the exacate longitude and latitude of a specific point add, the first two digits are used
        to later on access the full length
        """
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
        """
        iterates through the simplified shape vehicle position dictionary

        on each iteration pair of items are extracted and the necessary computations are performed on these
        """
        cumul_delay = 0
        for index1 in self.vehicle_positions:
            lines = self.vehicle_positions[index1]
            for index2 in lines:
                # one can see that there is index1 and index2 this reflects the fact
                data = lines[index2]['data']
                length = len(data)
                for i in range(1, length):
                    # extracting the pair of data
                    item1 = data[i - 1]
                    item2 = data[i]

                    try:
                        # the interest is centred when Specific vehicle reaches its destination
                        if item1['pointId'] != item2['pointId']:
                            distance = self.__calculate_distance(point1=str(item1['pointId']),
                                                                 point2=str(item2['pointId']),
                                                                 distance_point1=item1['distanceFromPoint'],
                                                                 distance_point2=item2['distanceFromPoint'])
                            speed, time1, time2 = self.__calculate_speed(distance, item1['time'], item2['time'], True)
                            if speed == 0: continue  # exclude the speed
                            expected_arrival_time, delay = self.__calculate__offline_time(item1['time'],
                                                                                          item2['pointId'],
                                                                                          distance)
                            # write to the CSV file
                            csv_writer.writerow(
                                [time2.strftime("%H:%M:%S"), expected_arrival_time, speed, item2['line_id']])


                        else:
                            continue

                    except:
                        continue

    def __calculate_distance(self, point1, point2, distance_point1=0, distance_point2=0, ):
        """
        :param point1: departure
        :param point2: arrival
        :param distance_point1: distance crossed by the vehicle from point 1
        :param distance_point2: distance crossed by the vehicle from point 2
        :return: returns the overall distance
        """
        try:
            first_digits = point1[:2]
            # extract the right coordinates
            info_point_id = self.stop_coords[first_digits][point1]
            departure_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            first_digits = point2[:2]
            info_point_id = self.stop_coords[first_digits][point2]
            # extract the right coordinates
            arrival_coord = (info_point_id['stop_lat'], info_point_id['stop_lon'])
            distance = geodesic(departure_coord, arrival_coord).meters - distance_point1 + distance_point2
            return distance
        except KeyError:
            return

    @staticmethod
    def __calculate_speed(distance, time1, time2, is_epoch_time=False):
        """
        :param distance:
        :param time1: departure time
        :param time2: arrival time
        :param is_epoch_time: whether the time passed as a param corresponds to linux epoch time
        :return: the speed , converted time
        """
        if is_epoch_time:
            time1 = time.strftime('%H:%M:%S', time.localtime(int(str(time1)[:10])))
            time2 = time.strftime('%H:%M:%S', time.localtime(int(str(time2)[:10])))

        time1 = datetime.strptime(time1, '%H:%M:%S')
        time2 = datetime.strptime(time2, '%H:%M:%S')
        time_diff = (time2 - time1).total_seconds()

        # compute distance
        if distance is None:
            return None
        else:
            return (distance / time_diff), time1, time2

    def set_offline_speed(self):
        """
        Generate the offline speed file, then load it to a dataframe

        """
        offline_speed = OfflineSpeed(stop_time_file_name=self.stop_time_file_name,
                                     stop_coords=self.stop_coords,
                                     compute_speed_func=self.__calculate_speed,
                                     compute_distance_func=self.__calculate_distance)

        file = offline_speed.container_file
        # if the offline speed file is empty then it need to be generated
        file_empty = os.stat(file).st_size == 0
        if not file_empty:
            self.offline_speed = pd.read_csv(file)

        else:
            offline_speed.generate_file()
            self.offline_speed = pd.read_csv(file)

    def __calculate__offline_time(self, departure_time, point_id, distance):
        """
        Computes the expected time which the vehicle is supposed to reach its destination

        :param departure_time: The vehicle departure time according to vehicle_position folder
        :param point_id:  The vehicle destination point id
        :param distance: The crossed distance by the vehicle
        """
        # since the offline speed is organized per point_id, it can be easily access
        # The dataframe has this shape
        """ 
            point_id,speed
            3612F,7.034272465365989
            3613F,7.126257234259174
            6799F,7.09789955668619
        """
        speed = self.offline_speed.loc[self.offline_speed['point_id'] == str(point_id)]['speed'].values[0]
        speed = float(speed)
        delay = distance / speed
        expected_time = timedelta(seconds=delay)
        # The expected time is transformed into suitable format to perform the necessary equation
        departure_time = time.strftime('%H:%M:%S', time.localtime(int(str(departure_time)[:10])))
        departure_time = datetime.strptime(departure_time, '%H:%M:%S')

        return (departure_time + expected_time).strftime("%H:%M:%S"), delay
