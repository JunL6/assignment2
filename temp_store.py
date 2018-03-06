'''
    operationalization for N = 1
'''
def define_trip(df_bin):
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the previous record
        list_triptable_1 = []   #a list to store the trip table, will be convert into dataframe later
        skip = 0
        num_cells = 0
        # num_cells = 1
        x_pre = -1
        y_pre = -1
        for  index, row in df_one_id.iterrows():
             # print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])
             # '''
            if(skip == 0):
                skip = 1
                time_start = row['record_time']
                x_pre, y_pre = row['xBin'], row['yBin']
                continue
            if(skip == 2):
                if(x_pre == row['xBin'] and y_pre == row['yBin']):
                    time_start = row['record_time']
                    continue
                else:
                    x_pre = row['xBin']
                    y_pre = row['yBin']
                    skip = 1
                    num_cells = num_cells + 1
                    time_end = row['record_time']
                    continue

            if(row['xBin'] == x_pre and row['yBin'] == y_pre and skip == 1):   #trip ends
                #1.get the duration of this trip
                duration = (time_end - time_start).total_seconds()/60
                #2.put this trip into list_triptable
                list_onetrip = [row['user_id'], num_cells, duration]
                list_triptable_1.append(list_onetrip)
                print(list_onetrip)
                #3. reset the varibales
                num_cells = 0
                # num_cells = 1
                skip = 2
                time_start = row['record_time']
            else:
                num_cells = num_cells + 1
                x_pre, y_pre = row['xBin'], row['yBin']
                time_end = row['record_time']

            print(row['xBin'], row['yBin'], row['record_time'], ", skip: ", skip, ", num_cell: ", num_cells)

    # print(list_triptable)
    return list_triptable_1
    # print('    ')
    # print(df_triptable_1)
    #



def define_trip_3(df_bin):
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the next record
        list_triptable_3 = []   #a list to store the trip table, will be convert into dataframe later
        skip = 0
        num_cells = 0
        # num_cells = 1
        x_pre = -1
        y_pre = -1
        n_stop = 1
        for  index, row in df_one_id.iterrows():
             # print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])
             # '''
            if(skip == 0):
                skip = 1
                time_start = row['record_time']
                x_pre, y_pre = row['xBin'], row['yBin']
                continue
            if(skip == 2):
                if(x_pre == row['xBin'] and y_pre == row['yBin']):
                    time_start = row['record_time']
                    continue
                else:
                    x_pre = row['xBin']
                    y_pre = row['yBin']
                    skip = 1
                    num_cells = num_cells + 1
                    time_end = row['record_time']
                    continue

            if(row['xBin'] == x_pre and row['yBin'] == y_pre and skip == 1):   #location is the same with last record
                #0. check if n_stop is 3 already
                if(n_stop <= 2):
                    x_pre, y_pre = row['xBin'], row['yBin']
                    time_end = row['record_time']
                    n_stop = n_stop + 1
                else:
                    #1.get the duration of this trip
                    duration = (time_end - time_start).total_seconds()/60
                    #2.put this trip into list_triptable
                    list_onetrip = [row['user_id'], num_cells, duration]
                    list_triptable_3.append(list_onetrip)
                    print(list_onetrip)
                    #3. reset the varibales
                    num_cells = 0
                    # num_cells = 1
                    skip = 2
                    time_start = row['record_time']
                    n_stop = 1
            else:
                num_cells = num_cells + 1
                x_pre, y_pre = row['xBin'], row['yBin']
                time_end = row['record_time']
                n_stop = 1


            print(row['xBin'], row['yBin'], row['record_time'], ", skip: ", skip, ", num_cell: ", num_cells, ", n_stop: ", n_stop)
    # print(list_triptable)
    return list_triptable_3
    # print('    ')
    # print(df_triptable_1)
    #



def define_trip_3(df_bin):
    #1. get same id
    for id in df_bin['user_id'].unique():
        df_one_id = df_bin.loc[df_bin['user_id'] == id]

        #2. get the trip: loop: compare the x, y of the current record with that of the previous record
        list_triptable_3 = []   #a list to store the trip table, will be convert into dataframe later
        num_cells = 0
        # num_cells = 1
        x_pre = df_one_id.iloc[0]['xBin']
        y_pre = df_one_id.iloc[0]['yBin']
        # print(x_pre, y_pre)
        time_start = 0
        stop = 1
        for  index, row in df_one_id.iterrows():
            #
            print(row['user_id'], row['xBin'], row['yBin'], row['record_time'])
            if(row['xBin']==x_pre and row['yBin']==y_pre):
                if(num_cells == 0):
                    print('跟上一条在同个地方，还没开始trip')
                    time_start = row['record_time']
                else:
                    if(stop > 2):
                        print("已经连续停留同个地方3次，trip结束")
                        #1. calculate duration
                        duration = (time_end - time_start).total_seconds()/60
                        #2. store one trip record
                        list_onetrip = [row['user_id'], num_cells, duration]
                        #
                        print(list_onetrip, "....................................................................")
                        list_triptable_3.append(list_onetrip)
                        #3. reset
                        num_cells = 0
                        time_start = row['record_time']
                        stop = 1
                    else:
                        print("跟上一条同个地方，trip中，但还没结束")
                        stop = stop + 1
                        time_end = row['record_time']

            else:
                print("跟上一条在不同地方，trip中")
                time_end = row['record_time']
                num_cells = num_cells + 1
                x_pre = row['xBin']
                y_pre = row['yBin']
                stop = 1
            print('....', 'num: ', num_cells, ', stop: ', stop)

    # print(list_triptable)
    return list_triptable_3
    # print('    ')
    # print(df_triptable_1)
    #
