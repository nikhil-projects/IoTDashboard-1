# coding: utf-8
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import numpy as np
import lnetatmo
from influxtsdb import InfluxTSDB
from lnetatmo import *

import time
from datetime import datetime

class Netatmo:
    def __init__(self, clientId, clientSecret, user, password, verbose=True):
        self.devList = WeatherStationData(lnetatmo.ClientAuth(clientId, clientSecret, user, password))
        self.ts_name = 'netatmo.{}{}'

    def set_timeseries_db(self, ts_db):
        self.ts_db = ts_db

    def get_start_timestamp(self, ts_name):
        try:
            start = self.ts_db.get_last_timestamp(ts_name).value / 10 ** 9 + 1
        except:
            start = int(time.time() - 3600 * 24 * 100)
        return start

    def import_missing(self):
        for station_id, station in self.devList.stations.items():
            station_name = station['station_name'].replace(' ', '_')
            ts = self.ts_name.format(station_name,'')
            start = self.get_start_timestamp(ts)
            mtype = ','.join(station['data_type'])
            resp = self.devList.getMeasure(device_id=station_id, module_id=None, scale="max",
                                           mtype=mtype, date_begin=start, date_end=time.time())
            df = self.convert_to_df(resp, station['data_type'])
            self.ts_db.write_ts(ts, df)

            for module in station['modules']:
                module_name = module['module_name'].replace(' ', '_')
                ts = self.ts_name.format(station_name, '.'+module_name)
                start = self.get_start_timestamp(ts)
                mtype = ','.join(module['data_type'])
                resp = self.devList.getMeasure(device_id=station_id, module_id=module['_id'], scale="max",
                                               mtype=mtype, date_begin=start, date_end=time.time())
                df = self.convert_to_df(resp, module['data_type'])
                if not df.empty:
                    self.ts_db.write_ts(ts, df)

    def convert_to_df(self, json, cols):
        numberOfRows = len(json['body'])
        columns = ['readtime', ]
        columns.extend(cols)
        df = pd.DataFrame(index=np.arange(0, numberOfRows), columns=columns)
        data = json['body'].items()
        for i in np.arange(0, numberOfRows):
            readtime, measures = data[i]
            try:
                measures = map(float, measures)
            except:
                continue
            row = [int(readtime)]
            row.extend(measures)
            df.loc[i] = row

        df.index = pd.to_datetime(df["readtime"], unit='s')
        df.index.name = "time"
        df = df.drop("readtime", 1)
        df.sort_index(ascending=False, inplace=True)
        df.dropna(axis=0, how='any', inplace=True)
        return df


if __name__ == '__main__':
    netatmo = Netatmo(clientId='592d4e574deddb17828b5207',
                      clientSecret='kiJelIlaYDXVPOBU8Vi1qPPBP2dRGNCupxXvKR1VRj7',
                      user='hagel@byg.dtu.dk',
                      password='DTUbyg402',
                      verbose=True)

    ts_db = InfluxTSDB(dbhost='localhost',
                         dbport=8086,
                         dbuser='root',
                         dbpassword='root',
                         dbname='testdb')
    ts_db.ensure_db()
    netatmo.set_timeseries_db(ts_db)

    sched = BlockingScheduler()

    netatmo.import_missing()

'''
    @sched.scheduled_job('interval', seconds=60)
    def poll_data():
        print('type=info msg="polling netatmo-meter" time="%s"' % datetime.now())
        netatmo.import_missing()

    # Start the schedule
    sched.start()
'''