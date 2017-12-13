import requests
import json
from decimal import Decimal
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


_BASEURL = "https://my.wirelesstag.net"
_SIGNIN = _BASEURL + "/ethAccount.asmx/SignIn"
_ISSIGNED = _BASEURL + "/ethAccount.asmx/IsSignedIn"
_GETTAGLIST = _BASEURL + "/ethClient.asmx/GetTagList"
_GETTEMPDATA = _BASEURL + "/ethLogShared.asmx/GetLatestTemperatureRawDataByUUID"
_GETTEMPCSV = _BASEURL + "/ethDownloadTempCSV.aspx"

_HEADERS = {
    "content-type": "application/json; charset=utf-8"
}

_DECIMALS = 1


class ClientAuth:
    """
    Request authentication and return authentication cookie. If cookie requested and not already logged in, it will log in again.
    """

    def __init__(self, username, password):

        postParams = {
            "email": username,
            "password": password
        }

        r = requests.post(_SIGNIN, headers=_HEADERS, data=json.dumps(postParams))
        self._accessCookie = r.cookies
        self._username = username
        self._password = password

        r = requests.post(_ISSIGNED, headers=_HEADERS, cookies=self._accessCookie)
        response = r.json()
        if response['d'] != True:
            raise ValueError('Incorrect Login operation')

    @property
    def accessCookie(self):
        # if not signed in, sign in and return cookie

        r = requests.post(_ISSIGNED, headers=_HEADERS)
        response = r.json()
        if response['d'] == 'TRUE':
            return self._accessCookie
        else:
            postParams = {
                "email": self._username,
                "password": self._password
            }

            r = requests.post(_SIGNIN, headers=_HEADERS, data=json.dumps(postParams))

            self._accessCookie = r.cookies
            return self._accessCookie

        return self._accessCookie


class WirelessTagData:
    """
    Retrieves data from Wireless senors available
    """

    def __init__(self, authData):
        self.getAuthToken = authData.accessCookie

    #   self._TagList = self.getTagsList()


    @property
    def tagList(self):
        self._tagList = {}
        cookies = self.getAuthToken
        r = requests.post(_GETTAGLIST, headers=_HEADERS, cookies=cookies)

        response = r.json()
        for i in response:
            for tag in response[i]:
                tag_id = tag["slaveId"]
                tag_uuid = tag["uuid"]
                tag_name = tag["name"]
                tag_type = tag["tagType"]

                self._tagList[tag_uuid] = {'tag_id': tag_id, 'tag_name': tag_name, 'tag_type': tag_type}

        return self._tagList

    def getLogInRange(self, uuid="", fromDate='', toDate=''):
        url = _GETTEMPCSV + '?uuid={}&fromdate={}&todate={}'.format(uuid, fromDate, toDate)
        res = requests.get(url)
        if res.status_code==200:
            # ['\xef\xbb\xbfDate/Time,Temperature (C),Moisture (%),Battery (Volts)', '2017-11-29T07:02:27+01:00,14.626259803772,70.1337280273438,2.8763861656189','']
            return res.content
        else:
            return None



        #r = requests.get(_GETTEMPCSV, params=json.dumps(data), headers=_HEADERS, cookies=cookies)
        #parsed_response = r.json()
        #print(r)
        #temp = Decimal(float(parsed_response["d"]["temp_degC"]))
        #rounded_temp = round(temp, _DECIMALS)
        #return rounded_temp


    def getTemperature(self, uuid=""):
        """
        If no UUID provided, it will take the first sensor discovered
        """

        if uuid == "":
            uuid = self.tagList.keys()[0]
        data = {
            "uuid": uuid
        }
        cookies = self.getAuthToken

        r = requests.post(_GETTEMPDATA, headers=_HEADERS, cookies=cookies, data=json.dumps(data))
        parsed_response = r.json()
        temp = Decimal(float(parsed_response["d"]["temp_degC"]))
        rounded_temp = round(temp, _DECIMALS)
        return rounded_temp

    def getHumidity(self, uuid=""):
        """
        If no UUID provided, it will take the first sensor discovered
        """

        if uuid == "":
            uuid = self.tagList.keys()[0]
        data = {
            "uuid": uuid
        }
        cookies = self.getAuthToken

        r = requests.post(_GETTEMPDATA, headers=_HEADERS, cookies=cookies, data=json.dumps(data))
        parsed_response = r.json()
        humid = Decimal(float(parsed_response["d"]["cap"]))
        rounded_humid = round(humid, _DECIMALS)

        return rounded_humid

    def getBatteryVolt(self, uuid=""):
        """
        If no UUID provided, it will take the first sensor discovered
        """

        if uuid == "":
            uuid = self.tagList.keys()[0]
        data = {
            "uuid": uuid
        }
        cookies = self.getAuthToken

        r = requests.post(_GETTEMPDATA, headers=_HEADERS, cookies=cookies, data=json.dumps(data))
        parsed_response = r.json()
        return parsed_response["d"]["battery_volts"]


class WirelessTag:
    def __init__(self, user, password, verbose=True, timeout=5000):
        self.ws = WirelessTagData(ClientAuth(user, password))
        self.timeout = timeout
        self.verbose = verbose
        self.ts_name = 'wirelesstag.{}'
        self.tz = pytz.timezone("UTC")

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


    def import_missing(self):
        self.save_tag_list()
        for uuid in self.uuids:
            start = self.get_start_timestamp(self.ts_name.format(uuid))
            if not start:
                start = int(time.time()) - 60 * 60 * 24 * 365
            stop = int(time.time())
            step = 60 * 60 * 24 * 7
            print start, stop
            for st in np.arange(start, stop, step):
                fromdate = datetime.fromtimestamp(st).isoformat().replace('+00:00', 'Z')
                todate = datetime.fromtimestamp(st + step).isoformat().replace('+00:00', 'Z')
                res = self.ws.getLogInRange(uuid, fromdate, todate)
                if res:
                    df = self.convert_to_df(res)
                    self.ts_db.write_ts(self.ts_name.format(uuid), df)

    def save_tag_list(self):
        self.uuids = self.ws.tagList.keys()
        for uuid in self.ws.tagList.keys():
            tag = self.ws.tagList[uuid]
            tag['uuid'] = uuid
            self.metadata_db.insert_one('wirelesstag', tag)


    def convert_to_df(self, s):
        if not s:
            return
        data = s.replace('"', '').split("\r\n")
        numberOfRows = len(data)-1
        columns = ['readtime', 'temperature', 'moisture', 'battery']
        df = pd.DataFrame(index=np.arange(0, numberOfRows-1), columns=columns)
        for i in np.arange(1, numberOfRows):
            values = data[i].split(',')
            if len(values)==4:
                row = []
                m = re.findall(r'(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)', values[0])[0]
                row_time = datetime(int(m[0]), int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5])).isoformat()
                row.append(row_time)
                row.append(float(values[1]))
                row.append(float(values[2]))
                row.append(float(values[3]))
                df.loc[i-1] = row
        df.index = pd.to_datetime(df["readtime"])
        df = df.drop("readtime", 1)
        df.index.name = "readtime"
        return df




if __name__ == '__main__':
    metadata_db = MongoConnection(host="localhost",
                            port=27017,
                            db_name='scadb',
                            username='sca',
                            password='Abcd1234')
    metadata_db.drop_table('wirelesstag')
    metadata_db.create_table('wirelesstag', index='uuid', unique=True)

    ts_db = InfluxTSDB(dbhost='localhost',
                       dbport=8086,
                       dbuser='root',
                       dbpassword='root',
                       dbname='scadb')
    ts_db.ensure_db()

    wirelesstag = WirelessTag(user='dummy',
                              password='dummy',
                              verbose=True,
                              timeout=5000)
    wirelesstag.set_metadata_db(metadata_db)
    wirelesstag.set_timeseries_db(ts_db)

    sched = BlockingScheduler()
    @sched.scheduled_job('interval', seconds=60*10)
    def poll_data():
        log.info('polling wirelesstag time=%s' % datetime.now())
    wirelesstag.import_missing()

    # Start the schedule
    sched.start()

