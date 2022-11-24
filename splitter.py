import csv
import os
import pandas as pd




def BPM_splitter(file, empLines = 5, header_num = 11): 

    chunk_list = []

    with open(file, 'r') as f:
        reader = [line.strip() for line in f.readlines()]

        front_index = 0
        i = 0
        data_temp = []

        while i < len(reader):
            # if reader[i] == ',,,,,,,,,,,,,,,,,,,,,,,,,,,' or i == len(reader)-1:
            if reader[i] == '' or i == len(reader)-1:
                data_temp = reader[front_index:i]
                chunk_list.append(data_temp)
                front_index = i + empLines
                i += empLines
            else:
                i += 1

    return chunk_list




def BPM_splitter_sep(file, empLines = 5, header_num = 11): 

# takes a large .csv BPM report of several days and generate two lists    
# chunk_list: list of daily data. each element is a list of strings. doesn't include the first 11 rows in each day.
# description_list: list of daily data descriptions. each element is a list of strings. only includes the first 11 rows in each day.

    chunk_list = []
    description_list =[]

    with open(file, 'r') as f:
        reader = [line.strip(':') for line in f.readlines()]

        front_index = 0
        i = 0
        new_temp = []
        header_temp = []

        while i < len(reader):
            # if reader[i] == ',,,,,,,,,,,,,,,,,,,,,,,,,,,' or i == len(reader)-1:
            if reader[i] == '' or i == len(reader)-1:
                data_temp = reader[front_index + header_num:i]
                header_temp = reader[front_index:front_index + header_num]
                chunk_list.append(data_temp)
                description_list.append(header_temp)
                front_index = i + empLines
                i += empLines
            else:
                i += 1

    return chunk_list, description_list




def BPM_transposer_CSV(file, header_num = 11): # takes a .csv file as input and spits out a dataframe and a date

    df1 = pd.read_csv(file, skiprows = header_num, low_memory = False)
    df2 = pd.read_csv(file, skiprows = None, nrows = header_num, header = None)
    df2_T = df2.T
    df2_T.columns = [i.strip(':') for i in df2.T.iloc[0]]
    df2_T = df2_T[1:]
    df2_T_repeat = pd.concat([df2_T] * df1.shape[0], ignore_index = True).iloc[:,:-1] # copy & paste values and repeat - this is slow
    df_out = pd.concat([df1, df2_T_repeat], axis=1)

    analysis_date = df2_T.iloc[0,1].replace('/', '-')

    return df_out, analysis_date




def BPM_transposer_list(lst, header_num = 11): # takes a list as input and spits out a dataframe and a date

    df1 = pd.DataFrame(data = [eachstr.split(',') for eachstr in lst[header_num +1 :]], columns = lst[header_num].split(','), index = None)
    df2 = pd.DataFrame([eachstr.split(',') for eachstr in lst[0:header_num - 1]])
    df2_T = df2.T
    df2_T.columns = [i.strip(':') for i in df2.T.iloc[0]]
    df2_T = df2_T[1:]
    df2_T_repeat = pd.concat([df2_T] * df1.shape[0], ignore_index = True).iloc[:,:-1]
    df_out = pd.concat([df1, df2_T_repeat], axis=1)

    analysis_date = df2_T.iloc[0,1].replace('/', '-')

    return df_out, analysis_date




def task(parm):    # expected format of argument is "{folder}!{subfolder}!{filename}"
    split = parm.split('!')
    folder = split[0]
    subfolder = split[1]
    file = split[2]
    result =[]
    for DailyData in BPM_splitter(f'{folder}/{file}'):
        transpose_result = BPM_transposer_list(DailyData)
        DailyDF = transpose_result[0]
        Analysis_date = transpose_result[1]
        DailyPath = f'{folder}/{subfolder}/{file[22:26]}'+'_'+ Analysis_date +'.csv'
        DailyDF.to_csv(DailyPath, sep = ',', index = False, quoting = None)