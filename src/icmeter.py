# coding: utf-8
import logging
import pytz
import re
import sys
import time
from datetime import datetime
from mongodb import MongoConnection

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from lnetatmo import *

from influxtsdb import InfluxTSDB

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('apscheduler.executors.default')


class IcMeter:
    def __init__(self, user, password, verbose=True, timeout=5000):
        self.user = user
        self.password = password
        self.timeout = timeout
        self.verbose = verbose
        self.ts_name = 'ic-meter.{}'

    def set_timeseries_db(self, ts_db):
        self.ts_db = ts_db

    def set_metadata_db(self, metadata_db):
        self.metadata_db = metadata_db

    def get_start_timestamp(self, ts_name):
        try:
            start = self.ts_db.get_last_timestamp(ts_name).value / 10 ** 9 + 1
        except:
            start = None
        return start

    def get_access_token(self):
        headers = {"user-agent": "curl/7.43.0"}
        url = "https://app.ic-meter.com/icm/oauth/token?client_id=trusted-client&grant_type=password&scope=read&username=%s&password=%s" % (
            self.user, self.password)
        r = requests.get(url, headers=headers, allow_redirects=False, timeout=self.timeout)
        if r.status_code == 200:
            session = json.loads(r.text)
            self.access_token = session['access_token']
            if self.verbose:
                print (session)
        else:
            raise Exception(r.text)

    def get_boxes(self):
        url = "https://app.ic-meter.com/icm/api/boxlocations?access_token=%s&_=%s" % (
            self.access_token, int(round(time.time() * 1000)))
        r = requests.get(url, allow_redirects=False, timeout=self.timeout)
        if r.status_code == 200:
            # save boxes metadata into MongoDB
            boxes = json.loads(r.text)
            #self.save_boxes(boxes)
            return boxes
        else:
            raise Exception(r.text)

    def save_boxes(self, boxes):
        for box in boxes:
            print(box)
            #self.metadata_db.update_upsert('ic_meters', {'boxid':box['boxId']}, box)

    def import_missing(self):
        self.get_access_token()
        boxes = self.get_boxes()

        for box in boxes:
            box_id = box['boxId']
            self.tz = pytz.timezone(box['timezone'])

            start = self.get_start_timestamp(self.ts_name.format(box_id))
            if start == None:
                start = int(box['fromdate'] / 1000)
            else:
                start = int(start)

            stop = int(time.time())
            if 'lastMeasurementDate' in box:
                stop = int(box['lastMeasurementDate'] / 1000)

            self.import_all_points(box_id, start, stop, 60 * 60 * 24 * 7)

    def import_all_points(self, box_id, start, stop, period=60 * 60 * 24 * 7):
        if start >= stop:
            print("No new data available for {}, skipping\n".format(box_id))
            return

        print("Downloading data of box %s  from %i to %i\n" % (box_id, start, stop))
        now = time.time()
        count = 0
        for f in range(start, stop, period):
            completed = 100.0 * (f - start) / (stop - start)
            delta = time.time() - now

            if self.verbose:
                if delta > 10:
                    timeleft = int((100.0 - completed) * (delta / completed))
                    print("Completed: %0.0f%% (%i seconds left)\n" % (completed, timeleft)),
                else:
                    print("Completed: %0.0f%%\n" % completed),
                sys.stdout.flush()

            data = self.get_data_period(box_id, f, period)
            if type(data) != pd.core.frame.DataFrame:
                time.sleep(3)
                print('Retrying...')
                data = self.get_data_period(box_id, f, period)

            if (type(data) == pd.core.frame.DataFrame) and (not data.empty):
                count += data.shape[0]
                self.ts_db.write_ts(self.ts_name.format(box_id), data)

        completed = (100 * (f - start)) / (stop - start)
        if self.verbose:
            print("Completed: %0.0f%%  \n  " % completed),
            delta = time.time() - now
        print("Type=info msg=\"Task completed\" elapsed_time=%0.0f rows_written=%i \n" % (delta, count))

    def get_data_period(self, box_id, start=1498720785, period=60 * 60 * 24 * 7.0):
        utc = pytz.timezone('UTC')
        fromdate = self.tz.normalize(self.tz.localize(datetime.fromtimestamp(start))).astimezone(
            utc).isoformat().replace(
            '+00:00', 'Z')
        todate = self.tz.normalize(self.tz.localize(datetime.fromtimestamp(start + period))).astimezone(
            utc).isoformat().replace(
            '+00:00', 'Z')

        timestamp = int(round(time.time() * 1000))
        data_url = "https://app.ic-meter.com/icm/api/measurements/days/%s?access_token=%s&fromDate=%s&toDate=%s&forecast=false&_=%i" % (
            box_id, self.access_token, fromdate, todate, timestamp)
        #print data_url

        r = requests.get(data_url, timeout=self.timeout)

        if r.status_code != 200:
            r = requests.get(data_url, timeout=self.timeout)
        if r.status_code != 200:
            r = requests.get(data_url, timeout=self.timeout)
        if r.status_code != 200:
            print('Failed to get data for boxid={}'.format(box_id))

        json = r.json()
        return self.convert_to_df(json)

    def convert_to_df(self, json):
        numberOfRows = len(json['rows'])

        columns = []
        col_types = []
        for col in json['cols']:
            columns.append(col['id'])
            col_types.append(col['type'])

        df = pd.DataFrame(index=np.arange(0, numberOfRows), columns=columns)
        for x in np.arange(0, numberOfRows):
            values = []
            for i, value in enumerate(json['rows'][x]['c']):
                if col_types[i] == 'datetime':
                    m = re.findall(r'Date\((\d+)\,(\d+)\,(\d+)\,(\d+)\,(\d+)\,(\d+)\)', value['v'])[0]
                    row_time = self.tz.localize(
                        datetime(int(m[0]), int(m[1]) + 1, int(m[2]), int(m[3]), int(m[4]), int(m[5]))).isoformat()
                    values.append(row_time)
                else:
                    if value['v'] == None:
                        values.append(float(0))
                    else:
                        values.append(float(value['v']))
            df.loc[x] = values
        df.index = pd.to_datetime(df["realtime"])
        df = df.drop("realtime", 1)
        df.index.name = "time"
        return df


if __name__ == '__main__':
    icmeter = IcMeter(user='*****',
                      password='*****',
                      verbose=True)

    ts_db = InfluxTSDB(dbhost='localhost',
                       dbport=8086,
                       dbuser='**',
                       dbpassword='****',
                       dbname='***')

    icmeter.set_timeseries_db(ts_db)
    icmeter.import_missing()




