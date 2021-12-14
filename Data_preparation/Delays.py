import ijson
import numpy
import pandas as pd
import datetime

vehicles = []
fetching_data = []
timestamp = []
backup = []
line_id = None
offline_timestamps = []
save = "trip_id, date, line_id, direction_id, delays\n"
error = None


def get_stop_name(stop):
    name = station_df.loc[station_df['stop_id'] == str(stop)]
    name.reset_index(inplace=True)
    return name['stop_name'][0]


def get_line_id(start_pos, stop_pos):
    # "GARE DE L'OUEST - STOCKEL" -> line ID = 1
    route_long_name = get_stop_name(start_pos) + ' - ' + get_stop_name(stop_pos)
    name_inverse = get_stop_name(stop_pos) + ' - ' + get_stop_name(start_pos)
    names = [route_long_name, name_inverse]
    line_id = lineId_df[lineId_df['route_long_name'].isin(names)]
    line_id.reset_index(inplace=True)
    if line_id.size != 0:
        # if line_id['route_short_name'][0] == '69':
         #   return None  # avoiding line 69
        return line_id['route_short_name'][0]
    global error
    error = names
    return None


def get_service_id(trip_id):
    service = trip_df.loc[trip_df['trip_id'] == trip_id]
    service.reset_index(inplace=True)
    return service['service_id'][0]


def get_trip_dates(service_id):
    date = calendar_df.loc[calendar_df['service_id'] == service_id]
    date.reset_index(inplace=True)
    # service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date

    monday, tuesday, wednesday = date['monday'][0], date['tuesday'][0], date['wednesday'][0]
    thursday, friday = date['thursday'][0], date['friday'][0]
    saturday, sunday, start_date = date['saturday'][0], date['sunday'][0], date['start_date'][0]
    end_date = date['end_date'][0]
    dates = generate_dates(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date)
    return dates


def generate_dates(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date):
    # 1, 1, 1, 1, 1, 0, 0, 20210823, 20210831
    days = [monday, tuesday, wednesday, thursday, friday, saturday, sunday]
    start_date = str(start_date)
    start_date = start_date[0:4] + '/' + start_date[4:6] + '/' + start_date[6:8]
    start_date += " " + clean_time(offline_times[0])  # cleaning all the time
    start_date = datetime.datetime.strptime(start_date, "%Y/%m/%d %H:%M:%S")

    end_date = str(end_date)
    end_date = end_date[0:4] + '/' + end_date[4:6] + '/' + end_date[6:8]
    end_date += " " + clean_time(offline_times[-1])  # cleaning all the time
    end_date = datetime.datetime.strptime(end_date, "%Y/%m/%d %H:%M:%S")

    dates = []
    real_data_start_time = datetime.datetime(2021, 9, 6, 9, 54)  # 2021-09-06 09:54:46  (exact time)
    real_data_stop_time = datetime.datetime(2021, 9, 21, 18, 19)  # 2021-09-21 18:19:13

    while start_date <= end_date:
        # need to check if the real time data covers the date or not
        if start_date > real_data_stop_time:
            break
        elif start_date.date() == real_data_stop_time.date() and days[start_date.weekday()] == 1:
            # check if we have all the trip records
            trip_ending_time = start_date.date() + " " + offline_times[-1]
            if trip_ending_time <= real_data_stop_time:
                dates.append(start_date.strftime('%d/%m/%Y %H:%M:%S'))
        elif days[start_date.weekday()] == 1 and start_date >= real_data_start_time:
            dates.append(start_date.strftime('%d/%m/%Y %H:%M:%S'))
        start_date += datetime.timedelta(days=1)
    return dates


def clean_time(time):
    """
    in some cases we have 24:00:00 -> 00:00:00
    25:00:00 -> 01:00:00
    :param time:
    :return:
    """
    new_time = time.split(':')
    if int(new_time[0]) == 24:
        time = "00:" + new_time[1] + ":" + new_time[2]
    elif int(new_time[0]) == 25:
        time = "01:" + new_time[1] + ":" + new_time[2]
    elif int(new_time[0]) == 26:
        time = "02:" + new_time[1] + ":" + new_time[2]
    elif int(new_time[0]) == 27:
        time = "03:" + new_time[1] + ":" + new_time[2]
    return time


def dates_to_timestamps(dates):
    timestamps = []
    for date in dates:
        time = datetime.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")
        timestamp = time.timestamp() * 1000  # in millisec
        timestamps.append(int(timestamp))
    return timestamps


def get_previous_timestamps(timestamps):
    previous_timestamps = []
    pos = 0
    for h in range(len(times)):
        if timestamps[pos] <= int(times[h][-1]):
            for v in range(len(times[h])):
                if int(times[h][v]) > timestamps[pos]:
                    if v != 0:
                        previous_timestamps.append([h, v - 1, int(times[h][v - 1])])
                        pos += 1
                        if pos == len(timestamps):
                            break
                    else:
                        previous_timestamps.append([h, v, int(times[h][v])])
                        pos += 1
                        if pos == len(timestamps):
                            break
        if pos == len(timestamps):
            break
    return previous_timestamps


# refresh vehicles
def refresh_vehicles(file_ac, time_p, l_id):
    """
    Find all vehicles present at a time (tim_p) on a particular line (l_ID)
    :param file_ac:
    :param time_p: time position in times
    :param l_id: line Id
    :return:
    """
    found = False
    vehicles = None
    if time_p < 0:  # first time issue
        time_p = 0
    for r in range(len(file_ac[0][time_p]['Responses'])):
        if file_ac[0][time_p]['Responses'][r] is not None:
            for d in range(len(file_ac[0][time_p]['Responses'][r]["lines"])):  # per line Id records
                if l_id == int(file_ac[0][time_p]['Responses'][r]["lines"][d]["lineId"]):  # lineId
                    vehicles = file_ac[0][time_p]['Responses'][r]["lines"][d]["vehiclePositions"]
                    found = True
                    break
        if found:
            break
    return vehicles


def get_next_timestamp(timestamp):
    timestamp[1] += 1
    if timestamp[1] == len(times[timestamp[0]]):
        timestamp[0] += 1
        timestamp[1] = 0
    timestamp[2] = times[timestamp[0]][timestamp[1]]
    return timestamp


# find vehicle
def select_vehicle(pos, direction_id):
    selected = []
    global vehicles
    global fetching_data
    global timestamp
    global backup
    global line_id
    end = 0
    if vehicles is not None:  # None is the case when there is no line Id information found in Json
        for v in vehicles:
            if v['directionId'] == str(direction_id):
                if pos == 0 and v['pointId'] == str(stop_sequence[0]) and int(v['distanceFromPoint']) == 0:
                    fetching_data = [[timestamp[2], v]]  # taking the last position of the start station
                    return pos
                if v['pointId'] == str(stop_sequence[pos + 1]):
                    if int(v['distanceFromPoint']) == 0:
                        fetching_data.append([timestamp[2], v])
                        pos += 1  # the vehicle should arrive to the next station
                        backup = []  # clear backup
                        return pos
                    elif is_not_in_backup(v['pointId']):
                        backup.append([timestamp[2], v])
                elif len(stop_sequence) > pos + 2 and v['pointId'] == str(stop_sequence[pos + 2]):
                    if int(v['distanceFromPoint']) == 0:
                        fetching_data.append([timestamp[2], v])
                        pos += 2  # sometimes the vehicle skip 1 station
                        backup = []  # clear backup
                        return pos
                    elif is_not_in_backup(v['pointId']):
                        backup.append([timestamp[2], v])
                elif len(stop_sequence) > pos + 3 and v['pointId'] == str(stop_sequence[pos + 3]):
                    if int(v['distanceFromPoint']) == 0:
                        fetching_data.append([timestamp[2], v])
                        pos += 3  # sometimes the vehicle skip 2 stations
                        backup = []  # clear backup
                        return pos
                    elif is_not_in_backup(v['pointId']):
                        backup.append([timestamp[2], v])
                        for elem in backup:
                            fetching_data.append(elem)
                        backup = []
                        return pos + 3
                elif len(stop_sequence) > pos + 4 and v['pointId'] == str(stop_sequence[pos + 4]) and len(
                        backup) >= 2:
                    if int(v['distanceFromPoint']) == 0:
                        backup.append([timestamp[2], v])
                    for elem in backup:
                        fetching_data.append(elem)
                    backup = []
                    if int(v['distanceFromPoint']) != 0:
                        backup.append([timestamp[2], v])
                    return pos + 4
            if pos >= len(stop_sequence) - 4:
                # print(type(stop_sequence[pos]))
                if v['directionId'] == str(direction_id) and v['pointId'] in str(stop_sequence[pos:]):  # numpy.int64()
                    end += 1  # sometime the vehicle skip the terminus                             # sometimes str
        if pos >= len(stop_sequence) - 4 and end == 0:
            for elem in backup:
                fetching_data.append(elem)
            backup = []
            return len(stop_sequence) - 1
    return pos


def is_not_in_backup(point_id):
    global backup
    res = True
    for elem in backup:
        if elem[1]['pointId'] == point_id:
            res = False
            break
    return res


###################################
# new searching algo
target_stations = []
step = 0
R = 4  # range of the look ahead station
S = 3  # steps before adding a new station in target


def get_station_pos(point_id):
    position = None
    for s in range(len(stop_sequence)):
        if str(point_id) in str(stop_sequence[s]):
            position = s
            break
    return position


def refresh_target_stations(pos):
    global target_stations, step
    if step == 0:
        target_stations = []
        if len(stop_sequence[pos + 1:]) > R:
            for i in range(R):
                target_stations.append(stop_sequence[pos + i])
        else:
            target_stations = stop_sequence[pos:]
    else:
        new_target = get_station_pos(target_stations[-1])
        if new_target is not None:
            new_target += 1
            if step % S == 0 and new_target < len(stop_sequence):
                # target_stations = target_stations[1:]               # testing a new thing
                target_stations.append(stop_sequence[new_target])
    return


def nearest_vehicle(vehicle1, vehicle2):
    n_vehicle = vehicle1
    pos1 = get_station_pos(vehicle1['pointId'])
    pos2 = get_station_pos(vehicle2['pointId'])
    if pos2 < pos1:
        n_vehicle = vehicle2
    return n_vehicle


def select_vehicles2(pos, direction_id):
    global vehicles, fetching_data, timestamp, target_stations, step
    refresh_target_stations(pos)
    selected = None
    length = len(fetching_data)
    end = 0
    if vehicles is not None:  # None is the case when there is no line Id information found in Json
        for v in vehicles:
            if v['directionId'] == str(direction_id):
                if v['pointId'] in str(target_stations):
                    if selected is None:
                        selected = v
                    else:
                        selected = nearest_vehicle(selected, v)
                # End detection
                if pos >= len(stop_sequence) - 4 and v['pointId'] in str(stop_sequence[pos:]):  # numpy.int64()
                    end += 1  # sometime the vehicle skip the terminus

        if selected is not None:
            if len(fetching_data) == 0:
                fetching_data.append([timestamp[2], selected])
            elif len(fetching_data) == 1:
                if selected['pointId'] == fetching_data[0][1]['pointId']:
                    if selected['distanceFromPoint'] == 0:
                        fetching_data = [[timestamp[2], selected]]
                    else:
                        step = 0
                        return 1
                elif get_station_pos(selected['pointId']) > get_station_pos(fetching_data[0][1]['pointId']):
                    fetching_data.append([timestamp[2], selected])
                    pos += 1
            else:
                if selected['pointId'] != fetching_data[-1][1]['pointId']:
                    fetching_data.append([timestamp[2], selected])
    if length == len(fetching_data):    # no vehicle found
        step += 1
    else:                   # vehicle added
        step = 0
    if pos >= len(stop_sequence) - 4 and end == 0:      # End of fetching data
        step = 0
        return len(stop_sequence) - 1
    if selected is not None and step == 0:
        return get_station_pos(selected['pointId'])
    else:
        return pos


###################################


def get_offline_timestamps(offlinetimes, date):
    offline_timestamps = []
    for t in offlinetimes:
        offline_timestamp = date.split()[0] + " " + clean_time(t)
        offline_timestamp = datetime.datetime.strptime(offline_timestamp, "%d/%m/%Y %H:%M:%S")
        offline_timestamp = offline_timestamp.timestamp() * 1000  # in millisec
        offline_timestamps.append(int(offline_timestamp))
    return offline_timestamps


def clean_data(dirty_data):
    cleaned_data = []
    global stop_sequence
    stop_position = 0
    for data_point in dirty_data:
        if str(data_point[1]['pointId']) in str(stop_sequence[stop_position]):
            cleaned_data.append([data_point[0], data_point[1]['pointId']])
            stop_position += 1
        else:
            gap = get_gap(stop_position, data_point[1]['pointId'])
            if stop_position != 0:
                fill_data = get_data_to_fill(cleaned_data[-1][0], data_point[0], gap)
                for g in range(gap):
                    cleaned_data.append(fill_data[g])
                cleaned_data.append([data_point[0], data_point[1]['pointId']])
                stop_position += gap + 1
            else:  # first stations missing case
                delta_t = int(offline_timestamps[gap]) - int(offline_timestamps[0])
                delta_t = delta_t / gap
                for i in range(gap):
                    cleaned_data.append(
                        [int(int(dirty_data[0][0]) - (gap - i) * delta_t), 'estimated'])  # subtracting duration
                cleaned_data.append([data_point[0], data_point[1]['pointId']])
                stop_position += gap + 1

    if len(cleaned_data) != len(stop_sequence):
        # some last stops can still be missing
        gap = len(stop_sequence) - len(cleaned_data)        # number of missing last stops
        for i in range(gap):
            offline_delay = int(offline_timestamps[-(gap-i)]) - int(offline_timestamps[-(gap-i+1)])
            estimated_delay = int(cleaned_data[-1][0]) + offline_delay
            cleaned_data.append([estimated_delay, 'estimated'])
    return cleaned_data


def get_gap(pos, pointId):
    """
    It gives the number of missing values in the dirty data according to the offline stop_sequence
    :param pos: position in the dirty data where the point_id is not the correct one
    :param pointId: the point_id we should have in this pos
    :return: number of missing point_id tho have a correct stop_sequence
    """
    global stop_sequence
    gap = 0
    for stop in stop_sequence[pos:]:
        if pointId not in str(stop):
            gap += 1
        else:
            break
    return gap


def get_data_to_fill(t1, t2, gap):
    """
    Calculates the difference between two timestamps and generate points to fill the missing values
    :param t1: starting time
    :param t2: end time
    :param gap: number of missing values
    :return:
    """
    fill = []
    estimated_delay = (int(t2) - int(t1)) / (gap + 1)
    start = float(t1)
    for i in range(gap):
        start += estimated_delay
        fill.append([int(start), 'estimated'])
    return fill


def get_trip_delays(data_collected):
    trip_delays = []
    global offline_timestamps
    if len(offline_timestamps) == len(data_collected):
        for t in range(len(offline_timestamps)):
            delay = int(data_collected[t][0]) - int(offline_timestamps[t])
            trip_delays.append(delay)
    else:
        print("Error occurred can't calculate the delays not the same length!")
        trip_delays = "ERROR"

    return trip_delays


def save_line(trip_id, date, line_id, direction_id, delays):
    return str(trip_id) + ',' + str(date) + ',' + str(line_id) + ',' + str(direction_id) + ',' + str(delays) + '\n'


def analyse_data(trip_id, stop_sequence, offline_times):
    print("trip_id = ", trip_id, "\nstop_sequence = ", stop_sequence)
    # Find the service id from the trip id
    service_id = get_service_id(trip_id)  # we don't really need to search for the service id it is in the trip Id

    # Find the dates of the trips
    dates = get_trip_dates(service_id)
    # Find the line id
    direction_id = stop_sequence[-1]
    global line_id
    line_id = get_line_id(stop_sequence[0], direction_id)
    print("line_Id = ", line_id)

    # if we have real time data dor those dates search can start
    if len(dates) != 0 and line_id is not None:
        print(dates)

        # Transform it into timestamp then get the previous timestamps
        # trip_start_time = offline_times[0]
        timestamps = dates_to_timestamps(dates)
        timestamps = get_previous_timestamps(timestamps)
        print(timestamps)

        # searching for realtime data
        real_time_data = []
        global vehicles
        global timestamp
        global fetching_data
        global target_stations
        global step
        for ts in timestamps:
            searching = True
            timestamp = ts
            max_timestamp = datetime.datetime.fromtimestamp(timestamp[2]/1000)
            max_timestamp += datetime.timedelta(days=1)
            max_timestamp = int(datetime.datetime.timestamp(max_timestamp) * 1000)     # trip is not more than one day
            pos = 0
            fetching_data = []
            target_stations = []
            step = 0
            while searching:
                vehicles = refresh_vehicles(file_access[timestamp[0]], timestamp[1],
                                            int(line_id))  # note it is timestamp[1]!
                pos = select_vehicles2(pos, direction_id)
                # print(target_stations)
                if pos == len(stop_sequence) - 1 or int(timestamp[2]) > max_timestamp:
                    searching = False
                else:
                    try:
                        timestamp = get_next_timestamp(timestamp)
                    except IndexError:
                        break
            real_time_data.append(fetching_data)
        # print(real_time_data)

        ##########################################################
        # Delay calculation

        global offline_timestamps
        global save
        for r in range(len(real_time_data)):

            # Transforming offline times into timestamp using the right date
            offline_timestamps = get_offline_timestamps(offline_times, dates[r])
            # print(offline_timestamps)
            data_collected = real_time_data[r]
            # print(data_collected)

            # Cleaning the collected data (if needed)
            if len(data_collected) != len(stop_sequence):
                print(data_collected)
                data_collected = clean_data(data_collected)
            print("clean = ", data_collected)

            # Delay calculation
            delays = get_trip_delays(data_collected)
            # print("Delays = ", delays)

            # Saving results
            save += save_line(trip_id, dates[r], line_id, direction_id, delays)
        # print(save)
    else:
        print("No real time data covering this trip : ", trip_id, dates)
        print("OR No real such route : ", error)


if __name__ == "__main__":

    ##########################################################
    # timestamp extraction from all json files
    files = ['vehiclePosition01.json', 'vehiclePosition02.json', 'vehiclePosition03.json', 'vehiclePosition04.json',
             'vehiclePosition05.json', 'vehiclePosition06.json', 'vehiclePosition07.json', 'vehiclePosition08.json',
             'vehiclePosition09.json', 'vehiclePosition10.json', 'vehiclePosition11.json', 'vehiclePosition12.json',
             'vehiclePosition13.json']
    times = []
    file_access = []
    print("Loading")
    test = 0
    for file in files:
        test += 1
        file_name = 'Data/' + file
        with open(file_name, 'r') as f:
            objects = ijson.items(f, "data")
            data = list(objects)
            file_access.append(data)
            # f.close()
        stamps = []
        for i in range(len(data[0])):  # per timeslot records
            time = data[0][i]['time']
            stamps.append(time)
        times.append(stamps)
        if test == -1:
            break
    print("Loaded")
    ##########################################################
    # dataFrame for searching a line ID
    lineId_df = pd.read_csv('Data/gtfs3Sept/routes.txt')
    lineId_df.drop(labels=['route_id', 'route_desc', 'route_type', 'route_url', 'route_color', 'route_text_color'],
                   axis=1, inplace=True)

    # dataFrame for searching station name
    station_df = pd.read_csv('Data/gtfs3Sept/stops.txt')
    station_df.drop(labels=['stop_code', 'stop_desc', 'stop_lat', 'stop_lon', 'zone_id', 'stop_url', 'location_type',
                            'parent_station'],
                    axis=1, inplace=True)

    ##########################################################
    # dataFrame for searching the service of a trip
    trip_df = pd.read_csv('Data/gtfs3Sept/trips.txt')
    trip_df.drop(labels=['route_id', 'trip_headsign', 'direction_id', 'block_id', 'shape_id'],
                 axis=1, inplace=True)

    # dataFrame for searching the dates of a service
    calendar_df = pd.read_csv('Data/gtfs3Sept/calendar.txt')

    ##########################################################
    # read stop_times
    df = pd.read_csv('test2.txt')
    df.drop(labels=['departure_time', 'pickup_type', 'drop_off_type'], axis=1, inplace=True)
    # print(df)

    stop_sequence = []
    offline_times = []
    trip_id = None
    new_file = open("results.txt", "a")
    Number_trip_test = 0  # testing
    for i in df.index:
        if trip_id is None:
            trip_id = df['trip_id'][i]
            stop_sequence.append(df['stop_id'][i])
            offline_times.append(df['arrival_time'][i])
        elif df['trip_id'][i] == trip_id:
            stop_sequence.append(df['stop_id'][i])
            offline_times.append(df['arrival_time'][i])
        else:
            Number_trip_test += 1
            if Number_trip_test == 200:
                print('writing in file')
                new_file.write(save)
                save = ""
                Number_trip_test = 0
            analyse_data(trip_id, stop_sequence, offline_times)
            trip_id = df['trip_id'][i]
            stop_sequence = [df['stop_id'][i]]
            offline_times = [df['arrival_time'][i]]
    analyse_data(trip_id, stop_sequence, offline_times)
    new_file.write(save)
    new_file.close()
    # print(save)
    print("End of computation...")
