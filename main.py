import os
import time

from data_preparation import data_loader
from delay_analysis import display_delay
from speed_analysis import data_cluster


def execute_data_preparation():
    data_direc = os.path.join(os.getcwd(), "Data", "gtfs23Sept")
    vehicle_position_folder = os.path.join(os.getcwd(), "Data", "vehiclePosition")

    stop_times_fname = os.path.join(data_direc, "stop_times.txt")  # stop times file name
    stops_fname = os.path.join(data_direc, "stops.txt")  # stop file name
    time_start = time.perf_counter()
    vehicle_position_files = os.listdir(vehicle_position_folder)
    for i in range(len(vehicle_position_files)):
        print(i)
        online_offline_data_file = os.path.join(os.getcwd(), "", "data_preparation",
                                                "generated_files", "vehicle_position" + str(i) + ".txt")
        file = os.path.join(vehicle_position_folder, vehicle_position_files[i])
        loader = data_loader.DataLoader(stop_time_file_name=stop_times_fname,
                                        stop_coords_file_name=stops_fname,
                                        vehicle_position_file=file,
                                        online_offline_data_file=online_offline_data_file)
        loader.load_stops()
        loader.set_offline_speed()
        loader.extract_offline_online_data()
    time_end = time.perf_counter()
    print(f"Time to load the data {time_end - time_start:0.4f} seconds")


def execute_delay_analysis():
    bus_file = os.path.join(os.getcwd(), "data", "generated_files", "delay_files", "bus_delays.txt")
    metro_tram_file = os.path.join(os.getcwd(), "data", "generated_files", "delay_files", "metro_tram_delays.txt")
    metro_tram_bus_file = os.path.join(os.getcwd(), "data", "generated_files", "delay_files",
                                       "bus_metro_tram_delays.txt")
    delay_files = [bus_file, metro_tram_file, metro_tram_bus_file]
    displayer = display_delay.DisplayDelay(delay_files=delay_files)
    displayer.parse_file()
    displayer.simplify_data()
    # displayer.plot_data()
    # displayer.plot_correlated_data()
    # displayer.plot_data_decomposition()
    displayer.start_forcasting()


def execute_speed_cluster():
    generated_files_folder = os.path.join(os.getcwd(), "data",
                                          "generated_files", "vehicle_positions")
    cluster = data_cluster.DataCluster(generated_files_folder)
    cluster.cluster_whole_data()


execute_speed_cluster()
