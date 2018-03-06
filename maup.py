import mysql.connector
from mysql.connector import Error
import pandas as pd

#display dataframe
pd.set_option('display.height',1000)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

#global dataframe
df_origindata = pd.DataFrame()
df_aggregateddata = pd.DataFrame(columns=['user_id', 'lat', 'lon', 'record_time'])

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
    # df_origindata = df_origindata.append(cursor.fetchmany(9999))
    df_origindata = df_origindata.append(cursor.fetchall())
    df_origindata.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']

### get same id record in one dataframe
def contain_sameid():
    df_singleid = pd.DataFrame()
    global df_origindata
    id_current = df_origindata['user_id'][0]
    df_singleid = df_singleid.append(df_origindata[0: 1])

    i = 1

    while i < len(df_origindata):
        record = df_origindata[i: i+1]

        #判断是否同一id
        if record['user_id'][i] == id_current:
            df_singleid = df_singleid.append(record)
        else:
            pass
            aggregate_data(df_singleid)
            id_current = record['user_id'][i]
            df_singleid.drop(df_singleid.index, inplace=True)
            df_singleid = df_singleid.append(record)
        i = i + 1

    aggregate_data(df_singleid)
###
#function: aggregate gps data
def aggregate_data(df_sameid):
    i = df_sameid.head(1).index.values[0]
    j_first = i
    df_singledc = pd.DataFrame()
    round = len(df_sameid) + i
    print('round: ', round)
    while i < round:
        df_singledc = df_singledc.append(df_sameid[i: i+1])
        # print((i+1)!=round)
        if (i+1) != round: #到底了s
            # print('i: ', i)
            # print(df_singledc)
            if (df_sameid['record_time'][i+1] -df_sameid['record_time'][j_first]).total_seconds() < 300:
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



#function: aggregate gps data within same ducy circle
def aggregate_singledc(df_singledc):
    # print(df_singledc)
    # print('...................................')
    lat_total = 0
    lon_total = 0
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





### main function
if __name__ == '__main__':
    # connect to the database
    connectdatabase()
    #  aggregate the data
    contain_sameid()
#   convert the gps data to UTM
    pass
#    generate a heatmap
