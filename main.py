from data_preparation import data_loader
import os
import time

separator = os.sep
data_direc = os.path.join(os.getcwd(), "Data", "gtfs23Sept")
vehicle_position = os.path.join(os.getcwd(), "Data", "vehiclePosition")

stop_times_fname = os.path.join(data_direc, "stop_times.txt")  # stop times file name
stops_fname = os.path.join(data_direc, "stops.txt")  # stop  file name
loader = data_loader.DataLoader(stop_time_file_name=stop_times_fname,
                                stop_coords_file_name=stops_fname,
                                vehicle_position_folder=vehicle_position)

# measuring the loading time
time_start = time.perf_counter()

loader.load_stops()
loader.set_offline_speed()
loader.extract_offline_online_data()
time_end = time.perf_counter()
print(f"Time to load the data {time_end - time_start:0.4f} seconds")
