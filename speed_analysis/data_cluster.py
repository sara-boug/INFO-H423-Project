import os
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from geopy.distance import geodesic


class DataCluster:
    """
    Contains the different functionalities to perform data clustering on specific JSON files

    This includes performing the necessary processing
    """
    def __init__(self, generated_files_folder):
        """

        :param generated_files_folder: The generated vehicle position folder  (data/generated_files/vehicle_positions)
        """
        self.generated_files_folder = generated_files_folder

        self.data_files = os.listdir(self.generated_files_folder)

        self.morning_dframe = pd.DataFrame()
        self.afternoon_dframe = pd.DataFrame()
        self.evening_dframe = pd.DataFrame()

        self.morning_dframe_file = os.path.join(os.getcwd(), "data", "generated_files", "day_period_data",
                                                'morning_dframe.txt')
        self.evening_dframe_file = os.path.join(os.getcwd(), "data", "generated_files", "day_period_data",
                                                'evening_dframe.txt')
        self.afternoon_dframe_file = os.path.join(os.getcwd(), "data", "generated_files", "day_period_data",
                                                  'afternoon_dframe.txt')

    def __load_data(self):
        """
        Loads the data from the folder data/generated_files/vehicle_positions/, then devides it into different time stamps
        """
        header_on = True
        write_mode = 'w'
        for file in self.data_files:
            path = os.path.join(self.generated_files_folder, file)
            dframe = pd.read_csv(path)
            dframe = dframe[['actual_time', 'speed', 'longitude', 'latitude']]
            dframe['actual_time'] = pd.to_datetime(dframe['actual_time'], format='%H:%M:%S',
                                                   infer_datetime_format=True)
            dframe['actual_time'] = dframe['actual_time'].dt.time

            # DataFrame condition for dividing the whole data into specific time interval
            morning_time = datetime(year=1900, month=1, day=1, hour=6, minute=00).time()
            noon_time = datetime(year=1900, month=1, day=1, hour=14, minute=00).time()
            evening_time = datetime(year=1900, month=1, day=1, hour=18, minute=00).time()
            # Apply the mask
            morning_mask = (dframe['actual_time'] >= morning_time) & (dframe['actual_time'] < noon_time)
            afternoon_mask = (dframe['actual_time'] >= noon_time) & (dframe['actual_time'] <= evening_time)
            evening_mask = (dframe['actual_time'] >= evening_time)

            self.morning_dframe = dframe.loc[morning_mask].sort_values(['actual_time'])

            self.afternoon_dframe = dframe.loc[afternoon_mask].sort_values(['actual_time'])
            self.evening_dframe = dframe.loc[evening_mask].sort_values(['actual_time'])
            self.load_locally(write_mode, header_on)
            header_on = False
            write_mode = 'a'

    def load_locally(self, write_mode, header_on):
        """
        Computes the distance for each dataframe row longitude and latitude

        :param write_mode: whether the file should be oppend in w or a
        :param header_on: whether to write header in the csv file
        """
        dframe_file_names = [self.morning_dframe_file, self.afternoon_dframe_file, self.evening_dframe_file]
        dframes = [self.morning_dframe, self.afternoon_dframe, self.evening_dframe]
        for i in range(len(dframe_file_names)):
            file_name = dframe_file_names[i]
            dframe = dframes[i]
            if dframe.empty:
                # if the data frame is empty then simply write the header and continue to the other dataframe
                dframe = dframe.drop(['actual_time', 'longitude', 'latitude'], axis=1)
                dframe['distance'] = ""
                dframe.to_csv(file_name, mode=write_mode, index=False, header=header_on)
                continue
            # the central coordinates of brussels
            center_coords = (4.3572, 50.8476)
            dframe = dframe.drop('actual_time', axis=1, inplace=False)
            dframe['distance'] = dframe.apply(lambda row: geodesic((row.longitude, row.latitude),
                                                                   center_coords).kilometers, axis=1)
            dframe = dframe.drop('longitude', axis=1, inplace=False)
            dframe = dframe.drop('latitude', axis=1, inplace=False)
            dframe.to_csv(file_name, mode=write_mode, index=False, header=header_on)

    @staticmethod
    def __cluster_data(dframe_file_name, axe, title):
        """
        Plots the dataframe according to the given axe

        :param dframe_file_name: the appropriate dataframe for the display
        :param axe: the plt axes
        :param title: the plot title
        """
        data = pd.read_csv(dframe_file_name)
        # filter the speed that lower than 0, this is considered more like noise
        data = data[(data['speed'] > 0)].to_numpy()
        dbscan = KMeans(n_clusters=4, n_init=3).fit(data)
        labels = dbscan.labels_
        unique_labels = set(labels)
        # assign colors according to the label to allow differentiating the cluster colors
        colors = [plt.cm.Spectral(each) for each in np.linspace(0, 1, len(unique_labels))]
        for label, color in zip(unique_labels, colors):
            if label == -1: continue
            # extract the clusters data
            class_member_mask = labels == label

            subdata = data[class_member_mask]
            axe.plot(
                subdata[:, 0],
                subdata[:, 1],
                "o",
                markerfacecolor=tuple(color),
                markeredgecolor="k",
                markersize=8,

            )
        axe.set_xlabel('Speed(m/s)')
        axe.set_ylabel('Distance from the center(km)')
        axe.set_title(title)

    def __plot_data(self):
        """
        Plots each data frame in the class

        """
        figure, axes = plt.subplots(3)
        self.__cluster_data(self.morning_dframe_file, axes[0], 'Morning Data [06:00, 14:00[')
        self.__cluster_data(self.afternoon_dframe_file, axes[1], 'Afternoon Data [14:00, 18:00[')
        self.__cluster_data(self.evening_dframe_file, axes[2], 'Evening Data [18:00, 06:00[')
        figure.tight_layout(pad=0.5)
        plt.show()

    def cluster_whole_data(self, ):
        """
        Searches for the files availability in the data/generated_files/day_period_data folder,
          - if they found, a clustering is performed on each folder
          - if they are not found, each file in data/vehicle_position is processed and the appropriate files
            are generated, to finally apply the clustering
        """
        try:
            self.__plot_data()
        except FileNotFoundError:
            print('The intended files are not found,they are loaded first')
            self.__load_data()
            print('Files loaded')
            self.__plot_data()
