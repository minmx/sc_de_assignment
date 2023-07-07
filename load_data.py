from dash import Dash, html, dcc, callback, Output, Input, DiskcacheManager
import diskcache
import pandas as pd
import plotly.express as px
import traceback
from uuid import uuid4

import data_processing_helper as helper
import params

sensor_list = list(params.sensor_info.keys())


# # Code written in pyspark below but not used for execution as unable to get Pyspark to run on personal laptop for testing and development
##################################################################################
# import pyspark
# from pyspark.sql import SparkSession, Window, functions as f
# from pyspark.ml.feature import MinMaxScaler
# from pyspark.ml import Pipeline
# from pyspark.ml.linalg import VectorAssembler

# spark = SparkSession.builder \
# .master('local') \
# .appName('read_sensor_data') \
# .config('spark.jars.packages') \
# .getOrCreate()
###################################################################################


if __name__ == '__main__':
    launch_uid = uuid4()

    # Diskcache for non-production when developing locally
    cache = diskcache.Cache("./cache")
    background_callback_manager = DiskcacheManager(
        cache, cache_by=[lambda: launch_uid], expire=60)

    # web app using Dash
    app = Dash(__name__, background_callback_manager=background_callback_manager)
    app.layout = html.Div([
        html.H1(children='Normalised Sensor Line Chart', style={'textAlign': 'center'}),
        dcc.Dropdown(sensor_list, sensor_list[0], id='dropdown-selection'),
        dcc.Graph(id='graph-content')])

    # preprocess all data for each sensor to reduce time taken when changing between sensors in dropdown options
    try:
        df = helper.get_data(params.file_loc)
        if df.shape[0] > 0:
            df1 = df[['sens_1']]
            df2 = df[['sens_2']]
            df4 = df[['sens_4']]
            df5 = df[['sens_5']]
            df_dict = {'Sensor 1': df1,
                       'Sensor 2': df2,
                       'Sensor 4': df4,
                       'Sensor 5': df5}
        else:
            df_dict = {'Sensor 1': pd.DataFrame(columns=['created_timestamp', 'sens_1']).set_index('created_timestamp'),
                       'Sensor 2': pd.DataFrame(columns=['created_timestamp', 'sens_2']).set_index('created_timestamp'),
                       'Sensor 4': pd.DataFrame(columns=['created_timestamp', 'sens_4']).set_index('created_timestamp'),
                       'Sensor 5': pd.DataFrame(columns=['created_timestamp', 'sens_5']).set_index('created_timestamp')}

    except Exception as e:
        exc = traceback.format_exc()
        exc = str(exc.replace('\n', ''))
        print(f'Error in processing data: {exc}')

    @callback(
        Output('graph-content', 'figure'),
        Input('dropdown-selection', 'value'),
        background=True)
    def update_graph(sensor):
        sensor_tag = params.sensor_info[sensor]
        dff = df_dict[sensor]
        fig = px.line(dff,
                    x=dff.index,
                    y=sensor_tag,
                    labels={'created_timestamp':'Created Timestamp', sensor_tag: sensor})
        fig.update_xaxes(range=[dff.index.min(), dff.index.max()])
        print(f'graph updated to {sensor}')
        return fig

    app.run_server(debug=True, host="0.0.0.0", port=8080)