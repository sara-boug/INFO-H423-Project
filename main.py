from data_preparation import data_loader
import os
import time

separator = os.sep
data_direc = os.path.join(os.getcwd(), "Data", "gtfs23Sept")

stop_times_fname = os.path.join(data_direc, "stop_times.txt")  # stop times file name
stops_fname = os.path.join(data_direc, "stops.txt")  # stop  file name
loader = data_loader.DataLoader(stop_times_file=stop_times_fname, stops_file=stops_fname)
# measuring the loading time
time_start = time.perf_counter()
loader.load_stops()
loader.load_stop_times()
time_end = time.perf_counter()
print(f"Time to load the data {time_end - time_start:0.4f} seconds")
