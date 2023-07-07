import pandas as pd
import traceback

import params

def read_data(file_loc):
    # sensor 1 - xlsx
    # sensor 2 - csv
    # sensor 4 - parquet
    # sensor 4 diff date - pickle
    # sensor 5 - json
    df = pd.DataFrame(columns=['created_timestamp', 'tag_key', 'tag_val', 'tag_quality'])
    try:
        s1_pd = pd.read_excel(file_loc['sensor1_file'])
        s2_pd = pd.read_csv(file_loc['sensor2_file'])
        s4_pd = pd.read_parquet(file_loc['sensor4_file'])
        s4_dd_pd = pd.read_pickle(file_loc['sensor4_dd_file'])
        s5_pd = pd.read_json(file_loc['sensor5_file'], orient='index')
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in reading files: {exc}')
        raise

    try:
        df = pd.concat([df, s1_pd, s2_pd, s4_pd, s4_dd_pd, s5_pd])
        print(s1_pd.shape, s2_pd.shape, s4_pd.shape, s4_dd_pd.shape, s5_pd.shape)
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in concatenating dataframes: {exc}')
        raise
    return df


def create_col_if_not_exist(df, col_name):
    if col_name not in df.columns:
        try:
            df[col_name] = []
        except Exception as e:
            exc = traceback.format_exc()
            exc = str(exc.replace('\n', ''))
            print(f'Error in creating new columns: {exc}')
            raise
    return df


def process_data(df):
    # filter Good quality data
    try:
        df = df[df['tag_quality']=='Good']
        df['created_timestamp'] = pd.to_datetime(df['created_timestamp'])
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in filtering dataframe: {exc}')
        raise

    # pivot table to set timestamp as index and tag names as columns
    try:
        df = pd.pivot_table(df, values='tag_val', index='created_timestamp', columns='tag_key')
        # ensure all 4 sensor columns are present in dataframe
        for col_name in params.sensor_info.values():
            df = create_col_if_not_exist(df, col_name)
        df = df.sort_index(axis=0)
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in pivoting dataframe: {exc}')
        raise

    # forward fill NA values
    try:
        df = df.fillna(method='ffill')
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in forward filling dataframe: {exc}')
        raise

    # scale data from 0-1, -> (value-min) / (max-min)
    try:
        for col in df.columns:
            df[col] = (df[col]-df[col].min())/(df[col].max()-df[col].min())
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in scaling dataframe: {exc}')
        raise
    return df


# read and process data
def get_data(file_loc):
    try:
        data = read_data(file_loc)
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace("\n",""))
        print(f'Error in get_data() function: {exc}')
        raise

    try:
        data = process_data(data)
    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace("\n",""))
        print(f'Error in process_data() function: {exc}')
        raise
    print('Final dataframe shape:', data.shape)
    return data


# # processing data with pyspark
# def process_data_pyspark(df):
#     df_pys = spark.createDataFrame(df)

#     # filter Good quality data
#     df_pys = df_pys.filter(df_pys.tag_quality=='Good')

#     # pivot table to set timestamp as index and tag names as columns
#     df_pys = df_pys.groupby('created_timestamp').pivot('tag_key').avg('tag_val')
#     df_pys = df_pys.sort('created_timestamp')

#     # forward fill NA values
#     window_last = Window.orderBy('created_timestamp')
#     for col_name, col_dtype in df_pys.dtypes:
#         df_pys = df_pys.withColumn(col_name, f.last(col_name, ignorenulls=True).over(window_last))

#     # scale data from 0-1, -> (value-min) / (max-min)
#     cols_to_scale = sensor_info.values()
#     assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in cols_to_scale]
#     scalers = [MinMaxScaler(inputCol=col+"_vec", outputCol=col+"_scaled") for col in cols_to_scale]
#     pipeline = Pipeline(stages=assemblers+scalers)
#     scalerModel = pipeline.fit(df_pys)
#     df_pys = scalerModel.transform(df_pys)

#     return df_pys