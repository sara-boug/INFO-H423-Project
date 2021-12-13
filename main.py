import os
import time

from data_preparation import data_loader
from data_analysis import display_delay


def Execute_data_preparation():
    separator = os.sep
    data_direc = os.path.join(os.getcwd(), "Data", "gtfs23Sept")
    vehicle_position_folder = os.path.join(os.getcwd(), "Data", "vehiclePosition")

    stop_times_fname = os.path.join(data_direc, "stop_times.txt")  # stop times file name
    stops_fname = os.path.join(data_direc, "stops.txt")  # stop  file name
    time_start = time.perf_counter()

    vehicle_position_files = os.listdir(vehicle_position_folder)
    i = 3

    for i in range(len(vehicle_position_files)):
        print(i)
        online_offline_data_file = os.path.join(os.getcwd(), "", "data_preparation/generated_files",
                                                "online_offline_files" + str(i) + ".txt")
        file = os.path.join(vehicle_position_folder, vehicle_position_files[i])
        loader = data_loader.DataLoader(stop_time_file_name=stop_times_fname,
                                        stop_coords_file_name=stops_fname,
                                        vehicle_position_file=file,
                                        online_offline_data_file=online_offline_data_file)
        loader.load_stops()
        loader.set_offline_speed()
        loader.extract_offline_online_data()
        i = i + 1
    time_end = time.perf_counter()
    print(f"Time to load the data {time_end - time_start:0.4f} seconds")


def execute_data_analysis():
    data_file = os.path.join(os.getcwd(), "Data", "results.txt")
    displayer = display_delay.DisplayDelay(data_file=data_file)
    displayer.simplify_data()
    # displayer.plot_data()
    # displayer.plot_correlated_data()
    # displayer.plot_data_decomposition()
    # displayer.start_forcasting()


execute_data_analysis()
