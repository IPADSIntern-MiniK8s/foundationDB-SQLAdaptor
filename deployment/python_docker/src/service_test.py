import fdb
fdb.api_version(710)
db = fdb.open('/var/fdb/fdb.cluster')
#db = dict()

def get_data(key):
    return db[key.encode()]

def write_data(key,value):
    db[key.encode()]=value.encode()

