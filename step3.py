import mysql.connector
from mysql.connector import Error
import pandas as pd
from pyproj import Proj

#display dataframe
pd.set_option('display.height',1000)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

#global dataframe
df_origindata = pd.DataFrame()
df_aggregateddata = pd.DataFrame(columns=['user_id', 'lat', 'lon', 'record_time'])
df_bindata = pd.DataFrame()


### get data
def connectdatabase():
    try:
        conn = mysql.connector.connect(user='jul077', password='abgEFIJXl_%Q17',
                                       host='crepe.usask.ca',
                                       database='SHED10')
        if conn.is_connected():
            print("PPP:Successfully connected to MYSQL database")
            cursor = conn.cursor()

            # get_gpsdata_oneoffilter50_saskatoon(cursor)
            get_data(cursor)

            cursor.close()
    except Error as e:
        print(e)
    finally:
        conn.close()
        print("PPP:Connection closed")



sql_gpsdata_filter50_saskatoon = 'select T2.user_id, T2.lat, T2.lon, T2.provider, T2.accu, T2.record_time, T2.date from (select T1.user_id, gps.lat, gps.lon, gps.provider, gps.accu, gps.record_time, gps.date from (select user_id, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 4176) as T1 left join gps on T1.user_id = gps.user_id) as T2 where T2.lat between 52.058367 and 52.214608 and T2.lon between -106.7649138128 and -106.52225318 and T2.accu<100'
def get_data(cursor):
    cursor.execute(sql_gpsdata_filter50_saskatoon)
    global df_origindata
    # df_origindata = df_origindata.append(cursor.fetchmany(127945))
    df_origindata = df_origindata.append(cursor.fetchmany(9999))
    # df_origindata = df_origindata.append(cursor.fetchmany(999))
    # df_origindata = df_origindata.append(cursor.fetchall())
    df_origindata.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']


'''
### get same id record in one dataframe
'''
def contain_sameid():
    # df_singleid = pd.DataFrame()
    global df_origindata
    # id_current = df_origindata['user_id'][0]
    #
    # df_singleid = df_singleid.append(df_origindata[0: 1])

    for id in df_origindata['user_id'].unique():
        df_one_user = df_origindata.loc[df_origindata['user_id'] == id]
        aggregate_data(df_one_user)

    # print(df_aggregateddata)

    # i = 1
    #
    # while i < len(df_origindata):
    #     record = df_origindata[i: i+1]
    #
    #     #判断是否同一id
    #     if record['user_id'][i] == id_current:
    #         df_singleid = df_singleid.append(record)
    #     else:
    #         pass
    #         aggregate_dg    #     i = i + 1
    #
    # aggregate_data(df_singleid)
###

'''
#function: aggregate gps data
'''
def aggregate_data(df_sameid):
    i = df_sameid.head(1).index.values[0]
    j_first = i
    df_singledc = pd.DataFrame()
    round = len(df_sameid) + i
    # print('round: ', round)
    while i < round:
        df_singledc = df_singledc.append(df_sameid[i: i+1])
        # print((i+1)!=round)
        if (i+1) != round: #到底了s
            # print('i: ', i)
            # print(df_singledc)
            if (df_sameid['record_time'][i+1] -df_sameid['record_time'][j_first]).total_seconds() < 270:
                pass
                # print(j_first, ', ', i, ': ', '<300')
            else:
                aggregate_singledc(df_singledc)
                # print(df_singledc)
                # print('..........................')
                df_singledc.drop(df_singledc.index, inplace=True)
                j_first = i+1
                # print(j_first, ', ', i, ': ', '>= 300')
        else:
            aggregate_singledc(df_singledc)

        i = i+1


'''
#function: aggregate gps data within same ducy circle
'''
def aggregate_singledc(df_singledc):
    # print(df_singledc)
    # print('...................................')
    lat_total = 0
    lon_total = 0

    if len(df_singledc)>1:
        i = df_singledc.head(1).index.values[0]
        user_id = df_singledc['user_id'][i]
        record_time = df_singledc['record_time'][i]
        round = len(df_singledc) + i
        while i < round:
            lat_total = lat_total + df_singledc['lat'][i]
            lon_total = lon_total + df_singledc['lon'][i]
            i = i+1

        lat_average = lat_total/len(df_singledc)
        lon_average = lon_total/len(df_singledc)

        global df_aggregateddata
        df_singleaggregateddate = pd.DataFrame(
            {'user_id': [user_id], 'lat': [lat_average], 'lon': [lon_average], 'record_time': [record_time]},
            columns=['user_id', 'lat', 'lon', 'record_time'])
        # print(df_singleaggregateddate)
        df_aggregateddata = df_aggregateddata.append(df_singleaggregateddate)


# This function aggregates the spatial position of all records within one duty cycle (5 minutes)
def aggregate_over_dutycycle(gps_traces):
   # We aggregate the GPS data by time, taking the average location every duty cycle
   # Loop through and if difference is greater than 300 seconds set new limit and take average and store in DF
   duty_cycle_aggregated = []

   # get Records for every user id
   for user_id in gps_traces.user_id.unique():
       # Sort the records based on record_time so we get in a sequential order
       gps_data_for_specific_user = gps_traces.loc[gps_traces['user_id'] == user_id].sort_values('record_time')
       # Let the first record be the threshold
       threshold_time = gps_data_for_specific_user.iloc[0].record_time
       aggregated_value = []

       # Every time we get a record that has a time difference of less than 250 seconds
       # than the threshold it means the record is in the same duty cycle so add it to store
       # if the difference is more it means we have switched to the next duty cycle so aggregate all values in the
       # store and update the threshold time to the new value
       for row in gps_data_for_specific_user.itertuples():
           if (row.record_time - threshold_time).total_seconds() > 270:
               temp_store = [float(sum(col)) / len(col) for col in zip(*aggregated_value)]
               duty_cycle_aggregated.append([user_id, threshold_time, temp_store[0], temp_store[1]])
               threshold_time = row.record_time
               aggregated_value = [[row.lat, row.lon]]
           else:
               aggregated_value.append([row.lat, row.lon])

       # Remaining values are pushed if any
       if len(aggregated_value) >= 1:
           temp_store = [float(sum(col)) / len(col) for col in zip(*aggregated_value)]
           duty_cycle_aggregated.append([user_id, threshold_time, temp_store[0], temp_store[1]])

   print("Aggregation Complete")
   # convert the list to a dataframe
   return pd.DataFrame(duty_cycle_aggregated, columns=["user_id", "record_time", "lat", "lon"])



'''
    convert UTM to cells in the grid of size 100m
'''
def binner(x, y):
    return (x-378999.99781991803)/100, (y-5768999.910592335)/100

'''
    convert lat, lon to UTM
'''
def convert_coord(df):

    # UTM code for Saskatoon
    p = Proj(init='EPSG:32613')

    # Converts from lat/long gps to UTM coordinates
    df['xord'], df['yord'] = p(df["lon"].values, df["lat"].values)
    df['xBin'], df['yBin'] = binner(df["xord"].values, df["yord"].values)
    df['xBin'] = df['xBin'].round()
    df['yBin'] = df['yBin'].round()

    return df

'''
    operationalization for N = 1
'''
def define_trip(df_bin):
    list_triptable_1 = []   #a list to store the trip table, will be convert into dataframe later
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the previous record
        num_cells = 0
        # num_cells = 1
        x_pre = df_one_id.iloc[0]['xBin']
        y_pre = df_one_id.iloc[0]['yBin']
        # print(x_pre, y_pre)
        time_start = 0
        for  index, row in df_one_id.iterrows():
            #
            # print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])
            if(row['xBin']==x_pre and row['yBin']==y_pre):
                if(num_cells == 0):
                    time_start = row['record_time']
                else:
                    #1. calculate duration
                    duration = (time_end - time_start).total_seconds()/60
                    #2. store one trip record
                    list_onetrip = [row['user_id'], num_cells, duration]
                    #
                    # print(list_onetrip)
                    list_triptable_1.append(list_onetrip)
                    #3. reset
                    num_cells = 0
                    time_start = row['record_time']
            else:
                time_end = row['record_time']
                num_cells = num_cells + 1
                x_pre = row['xBin']
                y_pre = row['yBin']

        print(list_triptable_1)
    # print(list_triptable)
    return list_triptable_1
    # print('    ')
    # print(df_triptable_1)
    #          '''

def define_trip_3(df_bin):
    list_triptable_3 = []   #a list to store the trip table, will be convert into dataframe later
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the previous record
        num_cells = 0
        # num_cells = 1
        x_pre = df_one_id.iloc[0]['xBin']
        y_pre = df_one_id.iloc[0]['yBin']
        # print(x_pre, y_pre)
        time_start = 0
        stop = 1
        for  index, row in df_one_id.iterrows():
            #
            # print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])

            if(row['xBin']!=x_pre or row['yBin']!=y_pre):
                # print("跟上一条在不同地方，trip中")
                time_end = row['record_time']
                num_cells = num_cells + 1
                x_pre = row['xBin']
                y_pre = row['yBin']
                stop = 1
            else:
                if (num_cells == 0):
                    # print('跟上一条在同个地方，还没开始trip')
                    time_start = row['record_time']
                else:
                    # print("跟上一条同个地方")
                    stop = stop + 1
                    time_end = row['record_time']
                    if (stop > 2):
                        # print("已经连续停留同个地方3次，trip结束")
                        # 1. calculate duration
                        duration = (time_end - time_start).total_seconds() / 60
                        # 2. store one trip record
                        list_onetrip = [row['user_id'], num_cells, duration]
                        #
                        # print(list_onetrip, "....................................................................")
                        list_triptable_3.append(list_onetrip)
                        # 3. reset
                        num_cells = 0
                        time_start = row['record_time']
                        stop = 1

            # print('....', 'num: ', num_cells, ', stop: ', stop)

    # print(list_triptable)
    return list_triptable_3
    # print('    ')
    # print(df_triptable_1)
    #


def define_trip_5(df_bin):
    list_triptable_5 = []   #a list to store the trip table, will be convert into dataframe later
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the previous record
        num_cells = 0
        # num_cells = 1
        x_pre = df_one_id.iloc[0]['xBin']
        y_pre = df_one_id.iloc[0]['yBin']
        # print(x_pre, y_pre)
        time_start = 0
        stop = 1
        for  index, row in df_one_id.iterrows():
            #
            # print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])

            if(row['xBin']!=x_pre or row['yBin']!=y_pre):
                # print("跟上一条在不同地方，trip中")
                time_end = row['record_time']
                num_cells = num_cells + 1
                x_pre = row['xBin']
                y_pre = row['yBin']
                stop = 1
            else:
                if (num_cells == 0):
                    # print('跟上一条在同个地方，还没开始trip')
                    time_start = row['record_time']
                else:
                    # print("跟上一条同个地方")
                    stop = stop + 1
                    time_end = row['record_time']
                    if (stop > 4):
                        # print("已经连续停留同个地方3次，trip结束")
                        # 1. calculate duration
                        duration = (time_end - time_start).total_seconds() / 60
                        # 2. store one trip record
                        list_onetrip = [row['user_id'], num_cells, duration]
                        #
                        # print(list_onetrip, "....................................................................")
                        list_triptable_5.append(list_onetrip)
                        # 3. reset
                        num_cells = 0
                        time_start = row['record_time']
                        stop = 1

            # print('....', 'num: ', num_cells, ', stop: ', stop)

    # print(list_triptable)
    return list_triptable_5
    # print('    ')
    # print(df_triptable_1)
    #


### main function
if __name__ == '__main__':
#### connect to the database
    connectdatabase()

####  aggregate the data
    print("Start aggregation...")
    # contain_sameid()

    df_aggregateddata = aggregate_over_dutycycle(df_origindata)
    # print(df_aggregateddata)
####   convert the gps data to UTM
    df_bindata = convert_coord(df_aggregateddata)
    print("Aggregation Finished!!!")
# test
    print("_____________df_bindata_________________")
    print(df_bindata)

#  operationalization

# #### N = 1
#     df_trip_1 = pd.DataFrame(data=define_trip(df_bindata), columns=['user_id', 'trip_num', 'duration'])
#     print('_________________________________trip table N=1___________________________________')
#     print(df_trip_1)


    # #N = 3
    # print('             ')
    # df_trip_3 = pd.DataFrame(data=define_trip_3(df_bindata), columns=['user_id', 'trip_num', 'duration'])
    # print(df_trip_3)
    #
    # #N = 5
    # print('             ')
    # df_trip_5 = pd.DataFrame(data=define_trip_5(df_bindata), columns=['user_id', 'trip_num', 'duration'])
    # print(df_trip_5)
