import mysql.connector
from mysql.connector import Error
import pandas as pd

#global dataframe
df_origindata = pd.DataFrame()

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
    df_origindata = df_origindata.append(cursor.fetchmany(12))
    df_origindata.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']

###compare time
def comparetime():
    # print(df_origindata['record_time'][0])
    delta = (df_origindata['record_time'][6] - df_origindata['record_time'][5]).total_seconds()
    if (df_origindata['record_time'][6] - df_origindata['record_time'][5]).total_seconds() < 300:
    # if delta < 300:
        print('<300')
    else:
        print('>=300')

    print(delta)
    # if delta < 300:
    #     print("delta < 300")
    # else:
    #     print("delta >= 300")
    # print(df_origindata.head(10))
    # print('type: ', type(delta))
    # # print(delta < 300)

    # delta_seconds = delta.total_seconds()
    # print(delta_seconds)






### main function
if __name__ == '__main__':
    connectdatabase()
    # contain_sameid()
    # df_origindata = df_origindata.sort_values(by=['user_id', 'record_time'])
    # comparetime()
    print(df_origindata)