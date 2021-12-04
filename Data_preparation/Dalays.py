import ijson
import pandas as pd
import datetime


def get_stop_name(stop):
    name = station_df.loc[station_df['stop_id'] == str(stop)]
    name.reset_index(inplace=True)
    return name['stop_name'][0]


def get_line_id(start_pos, stop_pos):
    # "GARE DE L'OUEST - STOCKEL" -> line ID = 1
    route_long_name = ""
    route_long_name += get_stop_name(start_pos)
    route_long_name += " - "
    route_long_name += get_stop_name(stop_pos)
    line_id = lineId_df.loc[lineId_df['route_long_name'] == route_long_name]
    line_id.reset_index(inplace=True)
    return line_id['route_short_name'][0]


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
    end_date = str(end_date)
    start_date = start_date[0:4] + '/' + start_date[4:6] + '/' + start_date[6:8]
    end_date = end_date[0:4] + '/' + end_date[4:6] + '/' + end_date[6:8]
    start_date = datetime.datetime.strptime(start_date, "%Y/%m/%d")
    end_date = datetime.datetime.strptime(end_date, "%Y/%m/%d")
    dates = []
    real_data_start_time = datetime.datetime(2021, 9, 6, 0, 0)  # 2021-09-06 00:00:00 (need to put the exact time)
    real_data_stop_time = datetime.datetime(2021, 9, 21, 0, 0)  # 2021-09-21 00:00:00
    # print('date should be between  : ', real_data_start_time, real_data_stop_time)
    end_date += datetime.timedelta(days=1)
    while start_date != end_date:
        # need to check if the real time data covers the date or not
        if start_date > real_data_stop_time:
            break
        if days[start_date.weekday()] == 1 and start_date >= real_data_start_time:
            dates.append(start_date.strftime('%d/%m/%Y'))
        start_date += datetime.timedelta(days=1)
    return dates


def dates_to_timestamps(trip_start_time, dates):
    timestamps = []
    for date in dates:
        time = date
        time += " "
        time += trip_start_time
        time = datetime.datetime.strptime(time, "%d/%m/%Y %H:%M:%S")
        timestamp = time.timestamp() * 1000  # in millisec
        timestamps.append(int(timestamp))
    return timestamps


def get_real_time_data(trip_id, timestamp):
    pass


if __name__ == "__main__":

    file_name = 'Data/vehiclePosition01.json'
    with open(file_name, 'r') as f:
        objects = ijson.items(f, "data")
        data = list(objects)

    ##########################################################
    # timestamp extraction for the json
    times = []
    for i in range(len(data[0])):  # per timeslot records
        time = data[0][i]['time']
        times.append(time)
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
    df = pd.read_csv('example.txt')
    df.drop(labels=['departure_time', 'pickup_type', 'drop_off_type'], axis=1, inplace=True)
    # print(df)
    trip_id = df['trip_id'][0]
    stop_sequence = []
    for i in df.index:
        stop_sequence.append(df['stop_id'][i])
    print("trip_id = ", trip_id, "\nstop_sequence = ", stop_sequence)

    # Find the line id
    line_id = get_line_id(stop_sequence[0], stop_sequence[-1])

    # Find the service id from the trip id
    service_id = get_service_id(trip_id)  # we don't really need to search for the service id it is in the trip Id

    # Find the dates of the trips
    dates = get_trip_dates(service_id)

    # Transform it into timestamp
    print(dates)
    trip_start_time = df['arrival_time'][0]
    timestamps = dates_to_timestamps(trip_start_time, dates)
    print(timestamps)


    timestamp = 1630962838000
    get_real_time_data(trip_id, timestamp)
