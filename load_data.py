import pandas as pd
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px

file_loc = {'sensor1_file': 'data_engineer_test/sensor_1.xlsx', 
            'sensor2_file': 'data_engineer_test/sensor_2.csv',
            'sensor4_file': 'data_engineer_test/sensor_4.parquet',
            'sensor4_dd_file': 'data_engineer_test/sensor_4_diff_date.pickle',
            'sensor5_file': 'data_engineer_test/sensor_5.json'}

sensor_info = {'Sensor 1': 'sens_1',
               'Sensor 2': 'sens_2',
               'Sensor 4': 'sens_4',
               'Sensor 5': 'sens_5',}

sensor_list = list(sensor_info.keys())


def read_data(file_loc):
    # sensor 1 - xlsx
    # sensor 2 - csv
    # sensor 4 - parquet
    # sensor 4 diff date - pickle
    # sensor 5 - json
    s1_pd = pd.read_excel(file_loc['sensor1_file'])
    s2_pd = pd.read_csv(file_loc['sensor2_file'])
    s4_pd = pd.read_parquet(file_loc['sensor4_file'])
    s4_dd_pd = pd.read_pickle(file_loc['sensor4_dd_file'])
    s5_pd = pd.read_json(file_loc['sensor5_file'], orient='index')

    df = pd.concat([s1_pd, s2_pd, s4_pd, s4_dd_pd, s5_pd])

    return df


def process_data(df):
    # filter Good quality data
    df = df[df['tag_quality']=='Good']
    df['created_timestamp'] = pd.to_datetime(df['created_timestamp'])

    # pivot table to set timestamp as index and tag names as columns
    df = pd.pivot_table(df, values='tag_val', index='created_timestamp', columns='tag_key')
    df = df.sort_index(axis=0)

    # forward fill NA values
    df = df.fillna(method='ffill')

    # scale data from 0-1, -> (value-min) / (max-min)
    for col in df.columns:
        df[col] = (df[col]-df[col].min())/(df[col].max()-df[col].min())

    return df


if __name__ == '__main__':
    # read and process data
    df = read_data(file_loc)
    df = process_data(df)

    # web app using Dash
    app = Dash(__name__)

    app.layout = html.Div([
        html.H1(children='Normalised Sensor Line Chart', style={'textAlign': 'center'}),
        dcc.Dropdown(sensor_list, sensor_list[0], id='dropdown-selection'),
        dcc.Graph(id='graph-content')])

    @callback(
        Output('graph-content', 'figure'),
        Input('dropdown-selection', 'value'))

    def update_graph(sensor):
        sensor_tag = sensor_info[sensor]
        dff = df[[sensor_tag]]
        # dff = dff[~dff[sensor_tag].isnull()]
        fig = px.line(dff,
                    x=dff.index,
                    y=sensor_tag,
                    labels={'created_timestamp':'Created Timestamp', sensor_tag: sensor})
        fig.update_xaxes(range=[dff.index.min(), dff.index.max()])
        return fig

    app.run_server(debug=True, host="0.0.0.0", port=8080)