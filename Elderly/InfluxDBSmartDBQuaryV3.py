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


def smartdb_Phaseshift_quary(host='127.0.01', port=8086, \
                   password = 'xxx', user = 'xxx', \
                   dbname = 'SyntheticSen.SyntheticSensorRetention', \
                   Start_quary_time = '2019-05-16T21:00:00.000Z', \
                   End_quary_time =  '2019-05-16T21:00:20.000Z', \
                   DeviceId_quary = '30099', \
                   Q_Tzone = 8, \
                   Print_Flag = 1 \
                   ):
    
    """Instantiate a connection to the InfluxDB."""
    # SELECT "pixels0" as "P11","pixels1" as "P12","pixels2" as "P13","pixels3" as "P14","pixels4" as "P15","pixels5" as "P16","pixels6" as "P17","pixels7" as "P18" FROM "SyntheticSensorRetention"."AMG8833_R1" WHERE ("DeviceId" = '30099') AND $timeFilter

    Start_quary_time = shift_quary_time(Start_quary_time,Q_Tzone)
    End_quary_time = shift_quary_time(End_quary_time,Q_Tzone)
      
    Str_pixels = 'pixels'
    Str_pixels_as = 'P'
    
    query_list = []
    
    for Row_Number in range(8):
      query = 'SELECT '
      for i in range(8):
          Str_pixels = 'pixels' + str(Row_Number * 8 + i)
          Str_pixels_as = 'P' + str(Row_Number + 1) + str(i + 1)
          query = query + ' \"' + Str_pixels + '\" as \"' + Str_pixels_as + '\"'
          if i != 7:
              query = query + ','
      query = query + ' FROM ' + '\"SyntheticSen\".\"SyntheticSensorRetention\".\"AMG8833_R' + str(Row_Number + 1) + '\"' + \
              ' WHERE (\"DeviceId\" = ' + '\'' + DeviceId_quary + '\') AND ' + \
              'time >' + '\'' + Start_quary_time + '\'' + ' AND ' + \
              'time <' + '\'' + End_quary_time + '\''
      query_list.append(query)
     
    client = InfluxDBClient(host, port, user, password, dbname)

    Influxdb_points_list = []
    
    # Debug Print Only
    if Print_Flag == 1:
      for i in range(8): 
        print("Querying data %d :" % (i+1) + query_list[i])
        Influxdb_result = client.query(query_list[i])
        Influxdb_points = list(Influxdb_result.get_points(measurement = ('AMG8833_R' + str(i + 1))))
        Influxdb_points_list.append(Influxdb_points)

    del Influxdb_result
    
    Data_Length = len(Influxdb_points_list[0])
    
    if Print_Flag == 1:
        print('Get %d data' % (Data_Length))
    
    res_pic_list = []
    time_list = []
    
    for i in range(Data_Length):
      temp_data = np.zeros((8,8))
      for Row_Number in range(8):
        for Column_Number in range(8):
          data_str = 'P' + str(Row_Number + 1) + str(Column_Number + 1)
          temp_data[Row_Number,Column_Number] = Influxdb_points_list[Row_Number][i][data_str]
      temp_time = Influxdb_points_list[Row_Number][i]['time']
      res_pic_list.append(temp_data)
      time_list.append(temp_time)                    
      
        
    return res_pic_list,time_list
