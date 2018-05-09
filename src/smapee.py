# coding: utf-8
import datetime as dt
import json

import arrow
import smappy
from postgres import Postgres


class Smapee:
    def __init__(self, clientId, clientSecret, username, password, verbose=True):
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.username = username
        self.password = password
        self.smapee = smappy.Smappee(clientId, clientSecret)
        self.smapee.authenticate(username, password)
        self.service_location_id = self.smapee.get_service_locations()['serviceLocations'][0]['serviceLocationId']

    def set_db(self, db):
        self.db = db

    def save_service_location_info(self):
        info = self.smapee.get_service_location_info(self.service_location_id)
        self.db.run('INSERT INTO smapee_service_info VALUES(%s, %s, %s, %s, %s, %s)', (self.service_location_id, self.clientId, self.clientSecret, self.username, self.password, json.dumps(info)))

    def get_start_timestamp(self, table_name):
        try:
            start = self.db.one('SELECT max("timestamp") as last_time FROM '+table_name+' WHERE serviceid=%(serviceid)s', {'serviceid':self.service_location_id})
            if start is None:
                raise Exception
        except Exception as e:
            start = dt.datetime.utcnow() - dt.timedelta(days=365)
        return start



    def import_5min_data(self):
        try:
            start = arrow.get(self.get_start_timestamp('smapee_elec_5min'), 'Europe/Copenhagen').to('utc').datetime + dt.timedelta(seconds=1)
            end = dt.datetime.utcnow()
            print('Import every  5min:', start, '-', end)
            df = self.smapee.get_consumption_dataframe(self.service_location_id, start=start, end=end, aggregation=1)
            df.reset_index(inplace=True)
            for row in df.to_dict(orient='records'):
                row['serviceid'] = self.service_location_id
                row['timestamp'] = arrow.get(row['timestamp'], 'utc').to('Europe/Copenhagen').datetime
                db.run('INSERT INTO smapee_elec_5min values(%(timestamp)s::timestamp without time zone, \
                   %(serviceid)s, %(alwaysOn)s, %(consumption)s, %(solar)s)', row)
        except Exception as e:
            print(e)

    def import_hourly_data(self):
        try:
            start = arrow.get(self.get_start_timestamp('smapee_elec_hourly'), 'Europe/Copenhagen').to('utc').datetime + dt.timedelta(hours=1)
            end = dt.datetime.utcnow()
            print('Import hourly:', start, '-', end)
            df = self.smapee.get_consumption_dataframe(self.service_location_id, start=start, end=end, aggregation=2)
            df.reset_index(inplace=True)
            for row in df.to_dict(orient='records'):
                row['serviceid'] = self.service_location_id
                row['timestamp'] = arrow.get(row['timestamp'], 'utc').to('Europe/Copenhagen').datetime
                db.run('INSERT INTO smapee_elec_hourly values(%(timestamp)s::timestamp without time zone, \
                   %(serviceid)s, %(alwaysOn)s, %(consumption)s, %(solar)s)', row)
        except Exception as e:
            print(e)

    def import_daily_data(self):
        try:
            start = arrow.get(self.get_start_timestamp('smapee_elec_daily')).datetime + dt.timedelta(days=1)
            end = dt.datetime.utcnow()
            print('Import daily:', start, '-', end)
            df = self.smapee.get_consumption_dataframe(self.service_location_id, start=start, end=end, aggregation=3)
            df.reset_index(inplace=True)
            for row in df.to_dict(orient='records'):
                row['serviceid'] = self.service_location_id
                row['timestamp'] = arrow.get(row['timestamp']).format(fmt='YYYY-MM-DD')
                db.run('INSERT INTO smapee_elec_daily values(%(timestamp)s::timestamp without time zone, \
                   %(serviceid)s, %(alwaysOn)s, %(consumption)s, %(solar)s)', row)
        except Exception as e:
            print(e)


    def get_appliance_start_timestamp(self, serviceid, applianceid):
        try:
            start = self.db.one('SELECT max("timestamp") as last_time FROM smapee_appliance_reading WHERE serviceid=%(serviceid)s and applianceid=%(applianceid)s',
                                {'serviceid':serviceid, 'applianceid':applianceid})
            if start is None:
                raise Exception
        except Exception as e:
            start = dt.datetime.utcnow() - dt.timedelta(days=365)
        return start


    def import_appliance_events(self):
        SQL = "select cast(json_array_elements(info->'appliances')->>'id' as integer) as appliance_id, " \
              "json_array_elements(info->'appliances')->>'name' as name " \
              "from smapee_service_info where serviceid=%(serviceid)s order by 1"
        rows = self.db.all(SQL, {'serviceid': self.service_location_id},  back_as=dict)
        end = dt.datetime.utcnow()
        for row in rows:
            applianceId = row.get('appliance_id')
            if applianceId:
                start = arrow.get(self.get_appliance_start_timestamp(self.service_location_id, applianceId), 'Europe/Copenhagen').to('utc').datetime + dt.timedelta(seconds=1)
                app_events = self.smapee.get_events(self.service_location_id, applianceId, start, end)
                for event in app_events:
                    event['serviceId'] = self.service_location_id
                    event['applianceId'] = applianceId
                    event['name'] = row.get('name')
                    event['timestamp'] = arrow.get(str(event['timestamp']/1000.0)).to('Europe/Copenhagen').datetime
                    if not 'totalPower' in event:
                        event['totalPower'] = None
                    self.db.run("INSERT INTO smapee_appliance_reading VALUES(%(serviceId)s, %(applianceId)s, %(timestamp)s::timestamp without time zone, %(name)s, %(totalPower)s, %(activePower)s)", event)


if __name__ == '__main__':
    accounts = [
         {'client_id':'Jens Andrsen', 'client_secret':'LmYUzYvLBJ', 'username':'Jens Andrsen', 'password':'DTUbyg402'},
         {'client_id':'hagel', 'client_secret':'grkxbUYndI', 'username':'hagel', 'password':'DTUbyg402'},
         {'client_id': 'kewin', 'client_secret': '3dDhxJaGbg', 'username': 'kewin', 'password': 'DTUbyg402'},
         {'client_id':'Jenny Eriksen', 'client_secret':'Khe6YfDr7S', 'username':'Jenny Eriksen', 'password':'DTUbyg402'},
         {'client_id':'annelise', 'client_secret':'ZC6UkLQUMa', 'username':'annelise', 'password':'DTUbyg402'}
    ]

    db = Postgres("postgres://xiuli:Abcd1234@193.200.45.38/testdb")
    for account in accounts:
        smapee = Smapee(clientId=account['client_id'],
                        clientSecret=account['client_secret'],
                        username=account['username'],
                        password=account['password'],
                        verbose=True)
        print('------ {} ------'.format(account['client_id']))
        smapee.set_db(db)
        #smapee.import_5min_data()
        #smapee.import_hourly_data()
        #smapee.import_daily_data()
        smapee.import_appliance_events()
