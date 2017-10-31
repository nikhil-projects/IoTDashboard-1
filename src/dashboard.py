import dash
from dash.dependencies import Input, Output, Event
import dash_core_components as dcc
import dash_html_components as html
import datetime
import plotly
import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go
from flask import Flask
from influxtsdb import InfluxTSDB

server = Flask('Dishboard')

dash.__version__
app = dash.Dash('Smart Meter Dashboard', server=server, url_base_pathname='/', csrf_protect=False)


app.layout = html.Div([

    html.Div([
        html.H1('IoTDashboard Dashboard')
              ],
             className="row"),

    html.Div([
        html.Div([dcc.Dropdown(
                id='iot-device',
                options=[{'label': 'IC-Meter', 'value':'ic-meter.5482'},
                         {'label': 'Netatmo-Net-2', 'value':'netatmo.Net-2'},
                         {'label': 'Netatmo-Net-2-Indoor', 'value':'netatmo.Net-2.Net-2_Indoor_module'},
                         {'label': 'Netatmo-Net-2-Outdoor', 'value':'netatmo.Net-2.Net-2_Outdoor_module'}
                        ], #ic-meter.5482, netatmo.Net-2, netatmo.Net-2.Net-2_Indoor_module, netatmo.Net-2.Net-2_Outdoor_module
                value='Choose IoTDashboard device')], className='col-6'),

        html.Div([
            dcc.Dropdown(
                id='iot-device-measure',
                # options=[{'label': s, 'value': s} for s in ['CO2', 'Humidity', 'NoiseAvg', 'NoisePeak', 'Temperature']],
                value='Choose the measure'
            )
        ], className='col-6')
    ], className="row"),

    html.Div([
        html.Div([ dcc.Graph(id='icmeter-live-update-graph')], className='col-12'),
        dcc.Interval(
            id='icmeter-interval-component',
            interval=60*1000 # in milliseconds
        )],
        className="row")

], className="container")


ts_db = InfluxTSDB(dbhost='localhost',
                   dbport=8086,
                   dbuser='root',
                   dbpassword='root',
                   dbname='testdb')


devices = {'ic-meter.5482': [('CO2', 'CO2'), ('Humidity', 'Humidity'), ('NoiseAvg', 'Average noise'), ('NoisePeak', 'Peak noise'), ('Temperature', 'Temperature')],
            'netatmo.Net-2' :[('CO2', 'CO2'), ('Humidity', 'Humidity'), ('Noise', 'Noise'), ('Pressure', 'Pressure'), ('Temperature', 'Temperature')],
            'netatmo.Net-2.Net-2_Indoor_module': [('CO2', 'CO2'), ('Humidity', 'Humidity'), ('Temperature', 'Temperature')],
            'netatmo.Net-2.Net-2_Outdoor_module': [('Humidity', 'Humidity'), ('Temperature', 'Temperature')]
          }

@app.callback(Output('iot-device-measure', 'options'),
              [Input('iot-device', 'value')],
              events=[Event('iot-device', 'change')])
def update_iot_device_measure(value):
    pairs = devices.get(value)
    return [{'label':label, 'value': value} for value, label in pairs]


# Multiple components can update everytime interval gets fired.
@app.callback(Output('icmeter-live-update-graph', 'figure'),
              [Input('iot-device', 'value'),
               Input('iot-device-measure', 'value')],
              events=[Event('icmeter-interval-component', 'interval'),
                      Event('iot-device-measure', 'change')])
def update_icmeter_graph_live(iot_device, iot_device_measure):
    df = ts_db.GetSeries(iot_device)
    ts = go.Scatter(
        x=df.index,
        y=df[iot_device_measure],
        name=iot_device_measure,
        line=dict(color='#1A237E'),
        #opacity=0.8
    )

    data = [ts]
    layout = dict(
        title='{} Time Series'.format(iot_device_measure),
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label='1m',
                         step='month',
                         stepmode='backward'),
                    dict(count=6,
                         label='6m',
                         step='month',
                         stepmode='backward'),
                    dict(step='all')
                ])
            ),
            rangeslider=dict(bgcolor='#F0F0F0',
                             borderwidth=0.5,
                             bordercolor='#E8FAFF',
                             thickness=0.08),
            type='date'
        )
    )

    fig = dict(data=data, layout=layout)
    return fig





external_js = [ "https://code.jquery.com/jquery-3.1.1.min.js",
    "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta/js/bootstrap.min.js",
    "https://code.highcharts.com/modules/exporting.js"
        ]

external_css = ["https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0-beta/css/bootstrap.min.css"]


for js in external_js:
    app.scripts.append_script({ "external_url": js })

for css in external_css:
    app.css.append_css({ "external_url": css})





if __name__ == '__main__':
    app.run_server(debug=True, host='::', port=8050)
    #ts_db = InfluxTSDB(dbhost='localhost',
    #                     dbport=8086,
    #                     dbuser='root',
    #                     dbpassword='root',
    #                     dbname='testdb')
    #ts = ts_db.GetSeries('ic-meter.5482')
    #print ts.CO2
    #print ts
    # timeseries: ic-meter.5482, netatmo.Net-2, netatmo.Net-2.Net-2_Indoor_module, netatmo.Net-2.Net-2_Outdoor_module