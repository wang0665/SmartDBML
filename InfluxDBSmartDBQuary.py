# -*- coding: utf-8 -*-
"""Wang Gucheng"""
"""InfluxDB Data Analysis"""

import time
import datetime
from influxdb import InfluxDBClient

def shift_quary_time(stime = '2018-05-27T00:00:00.000Z', tTZ = 8):
    Temp_Time = datetime.datetime.strptime(stime, "%Y-%m-%dT%H:%M:%S.000Z")
    Temp_Time = Temp_Time - datetime.timedelta(hours=tTZ)
    stime = Temp_Time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return stime

def shift_result_time(stime = '2018-05-27T17:00:00Z', tTZ = 8):
    Temp_Time = datetime.datetime.strptime(stime, "%Y-%m-%dT%H:%M:%SZ")
    Temp_Time = Temp_Time + datetime.timedelta(hours=tTZ)
    stime = Temp_Time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return stime


def smartdb_quary(host='127.0.01', port=8086, \
                   password = 'xxx', user = 'xxx', \
                   dbname = 'SDB.basicRetention', \
                   Start_quary_time = '2018-05-27T00:00:00.000Z', \
                   End_quary_time =  '2018-05-28T01:00:00.000Z', \
                   Channel_ID = 5, \
                   DeviceId_quary = '4177', \
                   Group_quary = '1m', \
                   Type_quary = 'RMSCurrent', \
                   Q_Tzone = 8, \
                   Print_Flag = 1 \
                   ):
    
    """Instantiate a connection to the InfluxDB."""
    # SELECT mean("RMSCurrent") AS "mean_Val" FROM "SDB"."basicRetention"."SecondlyReading" WHERE time > '2018-05-31T00:00:00.000Z' AND time < '2018-05-31T01:00:00.000Z' AND "ChannelId"='3' AND "DeviceId"='4177' GROUP BY time(5m)

    Channel_quary = str(Channel_ID)

    Start_quary_time = shift_quary_time(Start_quary_time,Q_Tzone)
    End_quary_time = shift_quary_time(End_quary_time,Q_Tzone)
    


    query = 'SELECT mean(\"' + Type_quary + '\") AS \"mean_Val\" FROM \"SDB\".\"basicRetention\".\"SecondlyReading\" WHERE ' + \
            'time >' + '\'' + Start_quary_time + '\'' + ' AND ' + \
            'time <' + '\'' + End_quary_time + '\'' +  ' AND ' + \
            '\"ChannelId\"=' + '\'' + Channel_quary + '\'' +  ' AND ' + \
            '\"DeviceId\"=' + '\'' + DeviceId_quary + '\'' + 'GROUP BY time(' + Group_quary + ')'

    # Debug Print Only
    # print("Host ip is: " + host)
    # print("Port is: " + str(port))

    client = InfluxDBClient(host, port, user, password, dbname)

    # Debug Print Only
    # print("Querying data: " + query)
    Influxdb_result = client.query(query)

    Influxdb_points = list(Influxdb_result.get_points(measurement = 'SecondlyReading'))

    del Influxdb_result
    
    if Print_Flag == 1:
        print('Get %d data' %(len(Influxdb_points)))

    Result_Array = [[0 for x in range(len(Influxdb_points))] for y in range(3)]

    for i in range(0, len(Influxdb_points)):
        Result_Array[1][i] = Influxdb_points[i]['mean_Val']
        Result_Array[2][i] = shift_result_time(Influxdb_points[i]['time'])
        Result_Array[0][i] = time.mktime(datetime.datetime.strptime(Result_Array[2][i], "%Y-%m-%dT%H:%M:%SZ").timetuple())

    del Influxdb_points
    return Result_Array






