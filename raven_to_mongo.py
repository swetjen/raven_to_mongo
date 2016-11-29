
###
# Used to update the web app with updated recall information from RavenDB.
###

from pymongo import MongoClient
import ravendb                 # DOCUMENTATION:  https://github.com/firegrass/ravendb-py
from tabulate import tabulate

UPDATE_DB = True               # Write Query to Mongo DB

# SOURCE SETTINGS
RAVEN_URL      = "http://urlToRaven:Port"
RAVEN_DB       = 'DatabaseName'
RAVEN_INDEXS   = ['Index1',
                  'Index2',
                  'Index3/Something/Else'] # These become our collections in Meteor...

# DESTINATION SETTINGS
MONGO_DEV_URL  = 'mongodb://127.0.0.1:3001/meteor'  # Local instance for Meteor...
MONGO_DEV_DB   = 'meteor'


def raven_id_to_mongo_id(store_object):
    # Accepts a store object and returns a mongo friendly ID substring.
    p = store_object['@metadata']['@id']
    return p[p.index('/')+1::1]

def viz_report(store,label):
    # Accepts a store and returns table summry of its contents
    result = []

    if label == 'mongo'.lower():
        for key,value in store.iteritems():
            y = [key,value]
            result.append(y)
    else:
        for key in store.keys():
            y = [key, str(len(store[key]))]
            result.append(y)

    return tabulate(result, headers=[label, 'Records'])


if __name__ == '__main__':
    # CREATE RAVEN OBJECT
    c1 = ravendb.store(url = RAVEN_URL, database = RAVEN_DB)
    session = c1.createSession()

    print '--------\nRAVEN DB\n--------\nURL: %s\nDB:  %s\n' % (RAVEN_URL, RAVEN_DB)

    # SAVE OUR QUERY RESULTS
    store = {}

    # ITERATE INDEXS AND STORE RESULTS
    for index in RAVEN_INDEXS:
        raven = session.query(index, query={})
        raven = raven['Results']
        store[index] = raven
        del raven

    # Show our source DB returned...
    print viz_report(store,'Raven')

    if UPDATE_DB:

        # Make our MONGO collection mames lowercase...
        for key in store.keys(): store[key.lower().translate(None,'/')] = store.pop(key)

        client  = MongoClient(MONGO_URL)
        db      = client.meteor

        tracking = {} # Save DB outputs so we can compare results.

        # Put stuff in Mongo!
        print '\n--------\nMONGO DB\n--------\nURL: %s\n' % (MONGO_URL)

        for key in store:
            try:
                db.drop_collection(key)
            except:
                pass # Probably a new db / first run.

            collection    = db[key]
            tracking[key] = 0

            for each in store[key]:

                x = each.copy()

                x['_id'] = raven_id_to_mongo_id(x)

                del x['@metadata'] # Hide the parts that make Mongo Angry.

                result_id = collection.insert_one(x).inserted_id
                tracking[key] += 1

        print viz_report(tracking,'mongo')
