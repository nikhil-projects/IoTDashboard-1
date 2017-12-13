#!/usr/bin/env python
# encoding: utf-8
# Create your views here.
from pymongo import MongoClient
import pymongo
import json
from bson import BSON
from bson import json_util


class MongoConnection():
    def __init__ (self, host="localhost", port=27017, db_name='scadb', username='sca', password='Abcd1234'):
        self.host = host
        self.port = port
        self.conn = MongoClient(self.host, self.port)
        self.db = self.conn[db_name]
        self.db.authenticate(username, password)

    def ensure_index(self, table_name, index=None):
        self.db[table_name].ensure_index([(index,pymongo.GEOSPHERE)])

    def create_table(self, table_name, index=None, unique=False):
        self.db[table_name].create_index([(index, pymongo.DESCENDING)], unique=unique)

    def drop_table(self, table_name):
        self.db.drop_collection(table_name)

    def get_one(self, table_name, conditions={}):
        single_doc = self.db[table_name].find_one(conditions)
        json_doc = json.dumps(single_doc,default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(json_doc)

    def get_all(self, table_name, conditions={}, sort_index ='_id', limit=100):
        all_doc = self.db[table_name].find(conditions).sort(sort_index, pymongo.DESCENDING).limit(limit)
        json_doc = json.dumps(list(all_doc),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(str(json_doc))

    def delete(self, table_name, conditions={}):
        self.db[table_name].remove(conditions)

    def insert_one(self, table_name, value):
        self.db[table_name].insert(value)

    def update_push(self, table_name, where, what):
        #print where, what
        self.db[table_name].update(where,{"$push":what}, upsert=False)

    def update(self, table_name, where, what):
        #print where, what
        self.db[table_name].update(where,{"$set":what}, upsert=False)

    def update_multi(self, table_name, where, what):
        self.db[table_name].update(where,{"$set":what}, upsert=False, multi=True)

    def update_upsert(self, table_name, where, what):
        self.db[table_name].update(where,{"$set":what}, upsert=True)


    def map_reduce(self, table_name, mapper, reducer, query, result_table_name):
        myresult = self.db[table_name].map_reduce(mapper, reducer, result_table_name, query)
        return myresult

    def map_reduce_search(self, table_name, mapper, reducer,query, sort_by, sort = -1, limit = 20):
        if sort_by == "distance":
            sort_direction = pymongo.ASCENDING
        else:
            sort_direction = pymongo.DESCENDING
        myresult = self.db[table_name].map_reduce(mapper,reducer,'results', query)
        results = self.db['results'].find().sort("value."+sort_by, sort_direction).limit(limit)
        json_doc = json.dumps(list(results),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(str(json_doc))

    def aggregrate_all(self, table_name, conditions={}):
        all_doc = self.db[table_name].aggregate(conditions)['result']
        json_doc = json.dumps(list(all_doc),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(str(json_doc))

    def group(self, table_name, key, condition, initial, reducer):
        all_doc = self.db[table_name].group(key=key, condition=condition, initial=initial, reduce=reducer)
        json_doc = json.dumps(list(all_doc),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(str(json_doc))

    def get_distinct(self, table_name, distinct_val, query):
        all_doc = self.db[table_name].find(query).distinct(distinct_val)
        count = len(all_doc)
        parameter = {}
        parameter['count'] = count
        parameter['results'] = all_doc
        return parameter

    def get_all_vals(self, table_name, conditions={}, sort_index ='_id'):
        all_doc = self.db[table_name].find(conditions).sort(sort_index, pymongo.DESCENDING)
        json_doc = json.dumps(list(all_doc),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(json_doc)

    def get_paginated_values(self, table_name, conditions ={}, sort_index ='_id', pageNumber = 1):
        all_doc = self.db[table_name].find(conditions).sort(sort_index, pymongo.DESCENDING).skip((pageNumber-1)*15).limit(15)
        json_doc = json.dumps(list(all_doc),default=json_util.default)
        json_doc = json_doc.replace("$oid", "id")
        json_doc = json_doc.replace("_id", "uid")
        return json.loads(json_doc)

    def get_count(self, table_name, conditions={}, sort_index='_id'):
        count = self.db[table_name].find(conditions).count()
        return count

    def close(self):
        self.conn.close()


if __name__ == '__main__':
    metadata_db = MongoConnection(host="193.200.45.37",
                            port=27017,
                            db_name='scadb',
                            username='admin',
                            password='Abcd1234')

    #metadata_db.drop_table('ic_meters')


    #metadata_db.create_table('ic_meters', index='boxid', unique=True)
    #metadata_db.insert_one('ic_meters', {'boxid': 1001, 'name': 'box1'})

    #metadata_db.delete('ic_meters', conditions={'name':'box2'})
    #print metadata_db.get_one('ic_meters', conditions={'boxid':1001})
    #print metadata_db.get_all('ic_meters')

