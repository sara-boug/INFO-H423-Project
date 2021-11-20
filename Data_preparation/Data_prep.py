import ijson
import pandas as pd
import numpy as np


########################################################
# functions definition

# refresh vehicles
def refresh_vehicles(time_p, l_id):
    """
    Find all vehicles present at a time (tim_p) on a particular line (l_ID)
    :param time_p: time position in times
    :param l_id: line Id
    :return:
    """
    found = False
    global vehicles
    if time_p < 0:      # first time issue
        time_p = 0
    for r in range(len(data[0][time_p]['Responses'])):
        if data[0][time_p]['Responses'][r] is not None:
            for d in range(len(data[0][time_p]['Responses'][r]["lines"])):  # per line Id records
                if l_id == int(data[0][time_p]['Responses'][r]["lines"][d]["lineId"]):  # lineId
                    vehicles = data[0][time_p]['Responses'][r]["lines"][d]["vehiclePositions"]
                    found = True
                    break
        if found:
            break
    return


# find vehicle
def select_vehicle(p_id, dist, d_id):
    """
    select the vehicle in the previous timestamp according to those information
    :param p_id: pointId
    :param dist: distanceFromPointId
    :param d_id: directionId
    :return: a vehicle position

    """
    selected = []
    for v in vehicles:                                           # make sure comparing ints not floats
        if v['pointId'] == p_id and v['directionId'] == d_id and int(v['distanceFromPoint']) <= int(dist):
            selected.append(v)
    if len(selected) == 1:
        return selected[0]
    elif len(selected) > 1:
        max_distance = 0
        res = None
        for dico in selected:
            if dico['distanceFromPoint'] >= max_distance:
                max_distance = dico['distanceFromPoint']
                res = dico
        return res
    else:
        # situation where a new vehicle is on the line but it is already moving so have a speed
        return {'directionId': d_id, 'distanceFromPoint': 0, 'pointId': p_id}


########################################################

if __name__ == "__main__":
    file_name = 'Data/vehiclePosition01.json'
    with open(file_name, 'r') as f:
        objects = ijson.items(f, "data")
        data = list(objects)

    ########################################################
    # extracting data from one json file into a table

    times = []
    table = []
    for i in range(len(data[0])):  # per timeslot records
        time = data[0][i]['time']
        times.append(time)
        for h in range(len(data[0][i]['Responses'])):
            if data[0][i]['Responses'][h] is not None:
                for j in range(len(data[0][i]['Responses'][h]["lines"])):  # per line Id records
                    lineId = data[0][i]['Responses'][h]["lines"][j]["lineId"]
                    for k in range(len(data[0][i]['Responses'][h]["lines"][j]["vehiclePositions"])):
                        vehicle = data[0][i]['Responses'][h]["lines"][j]["vehiclePositions"][k]
                        directionId = vehicle["directionId"]
                        distanceFromPoint = vehicle["distanceFromPoint"]
                        pointId = vehicle["pointId"]
                        table.append([int(time), lineId, pointId, int(distanceFromPoint), directionId])
    # we have a full table of one json file data
    # print(len(table))

    ########################################################
    # transforming the data to a pandas Dataframe

    dataFrame = pd.DataFrame(table, columns=["time", "lineId", "PointId", "Distance", "DirectionId"])
    print(dataFrame.head())

    # compute the speed of a vehicle

    speed = np.zeros(dataFrame.shape[0])  # initiate a numpy array
    vehicles = None

    ###
    # testing the selection_vehicle function
    # refresh_vehicles(0, 1)
    # vehicles.append({'directionId': '8162', 'distanceFromPoint': 2, 'pointId': '8282'})
    # print(vehicles)
    # print(select_vehicle('8282', 1, '8162'))
    ###

    times_pos = 0
    table_lineId = 0
    for index in dataFrame.index:

        # current position
        time = dataFrame['time'][index]
        lineId = dataFrame['lineId'][index]
        pointId = dataFrame['PointId'][index]
        distance = dataFrame['Distance'][index]
        directionId = dataFrame['DirectionId'][index]

        # refreshing the vehicles (time , lineId) if needed
        if int(lineId) != int(table_lineId):
            if int(times[times_pos]) != int(time):
                times_pos += 1
                # print("refresh table for time : ", times[times_pos], lineId)
            table_lineId = lineId
            refresh_vehicles(times_pos-1, int(table_lineId))

        # computing the speed:
        if distance == 0 or times_pos == 0:
            vehicleSpeed = 0
        else:
            # find vehicle
            found_vehicle = select_vehicle(pointId, distance, directionId)
            last_distance = found_vehicle['distanceFromPoint']
            vehicleSpeed = ((int(distance) - int(last_distance)) / (int(time) - int(times[times_pos-1]))) * 3600  # Km/h

        speed[index] = vehicleSpeed

    # Adding a column for speed in the table
    speed = pd.Series(speed)
    dataFrame['Speed'] = speed
    print(dataFrame)
