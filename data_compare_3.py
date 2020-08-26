import pandas as pd
# import pandavro as pdx
import datacompy
import xlrd
import os
from datetime import datetime
import xlsxwriter
import time
import numpy as np
import copy
import jason
import genson
import pandas as pd
import pandavro as pdx
import fastavro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader ,DatumWriter


def clean_key(val):
    if val.isnumeric():
        return ''
    else:
        str_to_list = val.split(",")
        # print(str_to_list)
        return str_to_list

def change_permissions_recursive(path, mode):
    for root, dirs, files in os.walk(path, topdown=False):
        for dir in [os.path.join(root,d) for d in dirs]:
            os.chmod(dir, mode)
    for file in [os.path.join(root, f) for f in files]:
            os.chmod(file, mode)

# reading user input

# input_excel_location='C:/Users/admin/PycharmProjects/DataComare/'
input_excel = 'C:/Users/admin/PycharmProjects/DataComare/SrcData/user_input.xlsx'  # User should put created file name and path here
# os.chdir(wrk_dir)
# os.getcwd()
df_inp = pd.read_excel(input_excel, sheet_name='csv_to_avro')
print(df_inp)

# Looping the test cases

for index, row in df_inp.iterrows():
    # reading user inputs values
    tc_name = row["Test Case Name"]
    wrk_dir = row["Work dir"]
    report_dir = row["Report_Location"]  # make sure this location is aleady created and exists
    src_f_path = row["Source File Path"]
    src_f_nm = row["Source File Name"]
    tgt_f_path = row["Target File Path"]
    tgt_f_nm = row["Target File Name"]
    unique_key = row["Unique_Keys"]
    run_flag = row["Run_Flag"]

    if str.upper(run_flag) == 'Y':
        os.chdir(src_f_path)
        os.getcwd()

        key = clean_key(str.lower(unique_key))

        # Source file path and reading into df

        src = src_f_path + src_f_nm
        TGT = tgt_f_path + tgt_f_nm
        #print(TGT)

        #src_df01 = pd.read_csv(src)  #csv read
        #fixed width file read
        data = pd.read_excel(input_excel, sheet_name='Input_schema')
        df = pd.DataFrame(data)  # df=pd.DataFrame(data,columns=['START_POSITION','END_POSITION'])
        #print(df)
        #df.query('Run_flag== "Y"', inplace=True)
        df = df.loc[df["Run_Flag"]=="Y"] #& df.loc[df["FILE_NAME"]=src]]
        df = df.loc[df["FILE_NAME"] == src]
        #print (df)

        dict_col_specs = {}  # Dictionary for Column Specs key = "filename", value =  [(0, 10), (11, 20), (21, 25), (26, 29)]
        dict_headers = {}  # Dictionary for headers; key = "filename, value = ['FIRST_NAME', 'LAST_NAME', 'BALANCE', 'INT_RATE']

        for index, row in df.iterrows():  # Parse per row in dataframe
            if row[0] not in dict_headers:  # If key:filename not present in dict
                dict_headers[row[0]] = [row[1]]
            else:  # If key:filename present in dict, then append to list of headers
                dict_headers[row[0]].append(row[1])

            if row[0] not in dict_col_specs:
                dict_col_specs[row[0]] = [(row[2], row[3])]
            else:
                    dict_col_specs[row[0]].append((row[2], row[3]))
                # print(type(row[2]))
        #print("headers", dict_headers)
        #print("col specs", dict_col_specs)
        col_specification=dict_col_specs.get(src)   #getthe value corrsponding to a key
        header = dict_headers.get(src)  #getthe value corrsponding to a key
        print("col_specification",col_specification)
        print("header", header)
        #for filename, header in dict_headers.items():  # Iterate over generated dictionary key:filename, value:header
            #col_specification = dict_col_specs[filename]  # Get value from key
        src_df01 = pd.read_fwf(src, colspecs=col_specification, names=header, index=True)
        #print(src_df01)
        # print(dict_headers.items())
        # newheaderDict= dict()
        # for (key,value) in dict_headers.items():
        #     if key== src:
        #         newheaderDict[key]=value
        # print('newheaderDict',newheaderDict)
        #
        # newColDict= dict()
        # for (key,value) in dict_col_specs.items():
        #     if key== src:
        #         newColDict[key]=value
        # print('newColDict',newColDict)
        # # for filename, header in newheaderDict.items():  # Iterate over generated dictionary key:filename, value:header
        # #     #col_specification = dict_col_specs[filename]  # Get value from key
        # col_specification = list(newColDict.values())  # Get value from key
        # header = list(newheaderDict.values())
        # print(header)
        # print(col_specification)
        # src_df01 = pd.read_fwf(src, colspecs=col_specification, names=header, index=True)
            #print(data)
                #data.to_csv('C:/Users/admin/PycharmProjects/DataComare/SrcData/sample.csv',index=False)
        #src_df01 = data
        #print(src_df01)
        src_df01.columns = map(str.lower, src_df01.columns)
        src_df01.sort_values(by=key, inplace=True)
        #print(src_df01)

        # with open(TGT,'rb') as fo:  #avro read
        #     target = fastavro.reader(fo)
        #     records = [r for r in target]
        #     tgt_df01 = pd.DataFrame.from_records(records)
        #     tgt_df01.columns = map(str.lower, tgt_df01.columns)
        #     #print(tgt_df01)
        file = []
        for filess in os.listdir(tgt_f_path):
            if os.path.isfile(os.path.join(tgt_f_path, filess)):
                # print(filess)
                file.append(filess)
        print(file)
        list = []
        for f in file:
            if f.endswith('.avro'):
                # print(tgt_f_path+f)
                with open(tgt_f_path + f, 'rb') as fo:  # avro read
                    target = fastavro.reader(fo)
                    records = [r for r in target]
                    # print(records)
                    target_df = pd.DataFrame.from_records(records)
                    target_df.columns = map(str.lower, target_df.columns)
                    list.append(target_df)
        tgt_df01 = pd.concat(list, axis=0, ignore_index=True, sort=True)
        #print(tgt_df01)
        #tgt_df01.to_csv('tgt.dat', index=False)
        tgt_df01.sort_values(by=key, inplace = True)
        #print(tgt_df01)

        print(key)

        count_of_src_rec = len(src_df01.index)
        count_of_tgt_rec = len(tgt_df01.index)

        df_dup_src = src_df01[src_df01.duplicated(subset=key,keep=False)]  # copying duplicate Pks of source in a csv

        df_dup_tgt = tgt_df01[tgt_df01.duplicated(subset=key,keep=False)]  # copying duplicate Pks of target in a csv
        # df_dup_tgt=df_dup_tgt.loc[:, key]  #select only keys

        df_OUTER_join = src_df01.merge(tgt_df01, on=key, how='outer', indicator=True)
        # print(df_OUTER_join)

        df_src_only = df_OUTER_join.loc[df_OUTER_join['_merge'] == "left_only"]
        # print(df_src_only)

        df_tgt_only = df_OUTER_join.loc[df_OUTER_join['_merge'] == "right_only"]
        # print(df_tgt_only)

        src_df01 = src_df01.drop_duplicates(subset=key , keep=False)
        tgt_df01 = tgt_df01.drop_duplicates(subset=key , keep=False) #keep false to remove all the duplicates
        #tgt_df01_1 = df_join.loc[df_join['_merge'] != "both"]
        #tgt_df01_1 = tgt_df01_1[tgt_df01_1.columns[:-1]]
        #tgt_df01_1.to_csv('after_dup_remove_tgt.dat', index=False)
        df_INNER_join = src_df01[key].merge(tgt_df01[key], on=key, how='inner', indicator=True)
        #df_INNER_join = df_INNER_join[df_INNER_join.columns[:-1]]
        #print(df_INNER_join)
        #print(df_INNER_join)
        df_INNER_join=df_INNER_join.drop(columns=['_merge'])
        #print(df_INNER_join)

        df_INNER_join = df_INNER_join.drop_duplicates(subset=None, keep='first', inplace=False)

        df_SRC_FINAL = src_df01.merge(df_INNER_join, on=key, how='inner', indicator=True)
        #print(df_SRC_FINAL)
        #df_SRC_FINAL = df_SRC_FINAL[df_SRC_FINAL.columns[:-1]]
        df_SRC_FINAL = df_SRC_FINAL.drop(columns=['_merge'])
        # df_SRC_FINAL.to_csv('comparing_src_dataset.dat', index=False)

        df_TGT_FINAL = tgt_df01.merge(df_INNER_join, on=key, how='inner', indicator=True)
        #df_TGT_FINAL = df_TGT_FINAL[df_TGT_FINAL.columns[:-1]]
        df_TGT_FINAL = df_TGT_FINAL.drop(columns=['_merge'])
        # df_TGT_FINAL.to_csv('comparing_tgt_dataset.dat', index=False)

        compare = datacompy.Compare(
            df_SRC_FINAL,
            df_TGT_FINAL,
            # on_index=True,
            # sample_count=100,
            join_columns=key,  # You can also specify a list of columns
            abs_tol=0,  # Optional, defaults to 0
            rel_tol=0,  # Optional, defaults to 0
            df1_name='Source',  # Optional, defaults to 'df1'
            df2_name='Target',  # Optional, defaults to 'df2'
            # report_name='COMPARE_RESULTS_'+Test_Entity+'_.xlsx',
            # clm_mismatch_report_name='Mismatches_'+Test_Entity+'_.xlsx',
            ignore_spaces=True,
            ignore_case=True
        )
        print("compare", compare.matches(ignore_extra_columns=True))  # returns true or false

        # current_dir = os.getcwd()
        print(report_dir)
        report_dir_individual_path = os.path.join(report_dir, 'Compare_' + tc_name)
        print(report_dir_individual_path)
        if not os.path.exists(report_dir_individual_path):
            os.makedirs(report_dir_individual_path)  # create new directory under eports folder for each testcase
            change_permissions_recursive(report_dir_individual_path, 0o777)
        else:
            os.chdir(report_dir_individual_path)
        os.chdir(report_dir_individual_path)

        report_nm = "Report_" + tc_name + "_"  + datetime.now().strftime("%Y%m%d_%H%M%S") + ".txt"
        # print(compare.report())
        with open(report_nm, 'w') as wf:
            wf.write(compare.report())
        print(df_dup_src)
        print("copying duplicate Pks of source in a csv ")
        df_dup_src.to_csv('Duplicate_row_in_src_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.dat', index=False)

        print("copying duplicate Pks of target in a csv ")
        df_dup_tgt.to_csv('Duplicate_row_in_tgt_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.dat', index=False)

        print("copying only target PK in csv file")
        df_tgt_only.loc[:, key].to_csv('only_target_pk_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.dat',
                                       index=False)  # write target minus source PK in only_target.dat
        print("copying only source PK in csv file")
        df_src_only.loc[:, key].to_csv('only_source_pk_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.dat',
                                       index=False)  # write source minus target PK in only_source.dat

        df_INNER_join.to_csv(
            'matched_pk_between_source_and_target_' + datetime.now().strftime("%Y%m%d_%H%M%S") + '.dat', index=False)

        df_OUTER_join = df_OUTER_join[0:0]  # empty the dataframe as it is not required now

        # Create Excel format Report
        df_summary = pd.DataFrame(columns=["Summary", "Count"])

        heading = ['Duplicate Rows in Src', 'Duplicate Rows in Tgt', 'Rows/Pks Only In Src', 'Rows/Pks Only In Tgt',
                   'No of Rows Compared', 'No Of Rows in Src', 'No Of Rows in Tgt', 'No Of Columns Compared'
            , 'No of Fields Mismatching']
        df_summary['Summary'] = heading
        data_in_count_column = [len(df_dup_src.index), len(df_dup_tgt.index), len(df_src_only.index),
                                len(df_tgt_only.index),
                                len(df_INNER_join.index), count_of_src_rec, count_of_tgt_rec, 0, 0]
        df_summary['Count'] = data_in_count_column
        #print(df_summary)

        report_nm_excel = "Report_" + tc_name + "_"  + datetime.now().strftime("%Y%m%d_%H%M%S") + ".xlsx"

        dummy = pd.DataFrame()

        writer = pd.ExcelWriter(report_nm_excel, engine='xlsxwriter')

        dummy.to_excel(writer, sheet_name='SUMMARY')
        dummy.to_excel(writer, sheet_name='ColumnWise_Result')
        dummy.to_excel(writer, sheet_name='Rows or Pks Only In Src')
        dummy.to_excel(writer, sheet_name='Rows or Pks Only In Tgt')
        dummy.to_excel(writer, sheet_name='Duplicate Rows in Src')
        dummy.to_excel(writer, sheet_name='Duplicate Rows in Tgt')

        colmns_compared = compare.intersect_columns()
        d = []
        for col in colmns_compared:
            if col not in compare.join_columns:
                df_mismatch = compare.sample_mismatch(col)
                df_mismatch.columns = df_mismatch.columns.str.replace('df1', 'Source')
                df_mismatch.columns = df_mismatch.columns.str.replace('df2', 'Target')
                # print(df_mismatch)
                if len(df_mismatch.index) != 0:
                    df_mismatch.to_excel(writer, sheet_name='Mismatch_' + col, index=False)
                    d.append(col)  # keep adding output to d

        df_mismatch_clm = pd.DataFrame(d, columns=['COLUMN'])
        print(df_mismatch_clm)
        print(colmns_compared)
        df_total_clm = pd.DataFrame(colmns_compared, columns=['COLUMN'])
        print(df_total_clm)
        df_clm_join = df_total_clm.merge(df_mismatch_clm, on='COLUMN', how='outer', indicator=True)
        #print(df_clm_join)
        df_clm_join['RESULTS'] = ['FAIL' if x == 'both' else 'PASS' for x in
                                  df_clm_join['_merge']]  # df_clm_join.apply(lambda row: row._merge=='both', axis=1)
        df_clm_join = df_clm_join.drop(columns=['_merge'])
        # print(df_clm_join)

        df_summary.loc[df_summary['Summary'] == 'No Of Columns Compared', 'Count'] = len(df_total_clm.index)
        df_summary.loc[df_summary['Summary'] == 'No of Fields Mismatching', 'Count'] = len(df_mismatch_clm.index)
        print(df_summary)

        df_summary.to_excel(writer, sheet_name='SUMMARY')
        df_clm_join.to_excel(writer, sheet_name='ColumnWise_Result')
        df_src_only.loc[:, key].to_excel(writer, sheet_name='Rows or Pks Only In Src', index=False)
        df_tgt_only.loc[:, key].to_excel(writer, sheet_name='Rows or Pks Only In Tgt', index=False)
        df_dup_src.to_excel(writer, sheet_name='Duplicate Rows in Src', index=False)
        df_dup_tgt.to_excel(writer, sheet_name='Duplicate Rows in Tgt', index=False)

        writer.save()

print("Script Execution Completed")
