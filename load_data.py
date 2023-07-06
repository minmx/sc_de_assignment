#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import findspark
# findspark.init()

# import pyspark
# from pyspark.sql import SparkSession


# In[ ]:


# spark = SparkSession.builder \
# .master('local') \
# .appName('read_sensor_data') \
# .config('spark.jars.packages", "com.crealytics:spark-excel_2.11:0.12.2') \
# .getOrCreate()


# In[1]:


import pandas as pd


# In[2]:


sensor1_file = 'data_engineer_test/sensor_1.xlsx'
sensor2_file = 'data_engineer_test/sensor_2.csv'
sensor4_file = 'data_engineer_test/sensor_4.parquet'
sensor4_dd_file = 'data_engineer_test/sensor_4_diff_date.pickle'
sensor5_file = 'data_engineer_test/sensor_5.json'


# In[4]:


# sensor 1 - xlsx
s1_pd = pd.read_excel(sensor1_file)
print(s1_pd.shape)

# # s1 pyspark
# s1_sp = spark.createDataFrame(s1_pd)


# In[5]:


# sensor 2 - csv
s2_pd = pd.read_csv(sensor2_file)
print(s2_pd.shape)


# In[6]:


# sensor 4 - parquet
s4_pd = pd.read_parquet(sensor4_file)
print(s4_pd.shape)


# In[7]:


# sensor 4 diff date - pickle
s4_dd_pd = pd.read_pickle(sensor4_dd_file)
print(s4_dd_pd.shape)


# In[8]:


# sensor 5 - json
s5_pd = pd.read_json(sensor5_file, orient='index')
print(s5_pd.shape)


# In[9]:


# store all sensor data in one dataframe
df = pd.concat([s1_pd, s2_pd, s4_pd, s4_dd_pd, s5_pd])
print(df.shape)


# In[10]:


# filter good quality data
df_good = df[df['tag_quality']=='Good']
df_good['created_timestamp'] = pd.to_datetime(df_good['created_timestamp'])
df_good


# In[38]:


# pivot table to set timestamp as index and tag names as columns
df_pivoted = pd.pivot_table(df_good, values='tag_val', index='created_timestamp', columns='tag_key')
df_pivoted = df_pivoted.sort_index(axis=0)
df_pivoted


# In[39]:


# forward fill NA values
df_ffill = df_pivoted.fillna(method='ffill')
df_ffill


# In[40]:


# scale data from 0-1
df_norm = df_ffill.copy()
for col in df_ffill.columns:
    df_norm[col] = (df_ffill[col]-df_ffill[col].min())/(df_ffill[col].max()-df_ffill[col].min())
#     df_ffill[col+'_norm'] = (df_ffill[col]-df_ffill[col].min())/(df_ffill[col].max()-df_ffill[col].min())


# In[63]:


# frontend framework to display data using dash
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
# from cachetools import cached, TTLCache

app = Dash(__name__)

df = df_norm
sensor_info = {'Sensor 1': 'sens_1',
              'Sensor 2': 'sens_2',
              'Sensor 4': 'sens_4',
              'Sensor 5': 'sens_5',}

sensor_list = list(sensor_info.keys())

app.layout = html.Div([
    html.H1(children='Normalised Sensor Data Visualisation', style={'textAlign':'center'}),
    dcc.Dropdown(sensor_list, sensor_list[0], id='dropdown-selection'),
    dcc.Graph(id='graph-content')
])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)

# cache = TTLCache(maxsize=100, ttl=86400)
# @cached(cache)

def update_graph(sensor):
    sensor_tag = sensor_info[sensor]
    dff = df[[sensor_tag]]
#     dff = dff[~dff[sensor_tag].isnull()]
    
    fig = px.line(dff,
                  x=dff.index,
                  y=sensor_tag,
                  labels={'created_timestamp':'Created Timestamp',
                            sensor_tag: sensor})
    fig.update_xaxes(range=[dff.index.min(), dff.index.max()])

    return fig


if __name__ == '__main__':
    app.run(debug=True)


# In[ ]:




