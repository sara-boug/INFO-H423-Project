import os

import pandas as pd
import csv


class OfflineSpeed:
    """
    The aim of this class is to generate a file having this format

    point_id,speed
    3612F,7.034272465365989
    3613F,7.126257234259174
    6799F,7.09789955668619
    """
    stop_time_file_name = ""  # stop times file name
    vehicle_position_folder = ""

    def __init__(self, stop_time_file_name, stop_coords, compute_distance_func, compute_speed_func):
        self.stop_time_file_name = stop_time_file_name
        self.stop_coords = stop_coords

        # the computer distance and seep are passed as a parameters
        self.compute_distance_func = compute_distance_func
        self.compute_speed_func = compute_speed_func
        # this dictionary will contain  point_id average offline speed
        self.container_file = os.path.join(os.getcwd(), "data_preparation", "generated_files", 'sample' , "offline_speed.txt")

    def generate_file(self):
        """
        Reads the stop_time and compute the speed between each ordered pair of point ids belonging to the same
        trip_id, then pass to a file for efficiency (to avoid loading a lot of data into memory )

        """
        # allows keeping track of the already inserted point ids, it is already seen, then the point id is ignored
        # to avoid redundant ids
        seen_point_id = []
        file = open(self.container_file, 'w')
        writer = csv.writer(file)
        writer.writerow(['point_id', 'speed'])
        attributes = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type",
                      "drop_off_type"]

        # load the stop time file into a dataframe
        data_frame = pd.read_csv(self.stop_time_file_name, usecols=attributes)
        # group by trip_id, so every chunk will have the same line_id
        data = data_frame.groupby(["trip_id"])

        for state, frame in data:
            data_array = frame.to_dict('records')
            length = len(data_array)
            for i in range(1, length):
                data1 = data_array[i - 1]
                data2 = data_array[i]

                # if the point id is already seen, no need to add it
                if data2['stop_id'] in seen_point_id:
                    continue
                distance = self.compute_distance_func(str(data1['stop_id']), str(data2['stop_id']))
                try:
                    speed = self.compute_speed_func(distance, data1['departure_time'], data2['arrival_time'])
                except ValueError:
                    continue

                writer.writerow([data2['stop_id'], speed])
                seen_point_id.append(data2['stop_id'])
