from flask import Flask,request
from service_test import get_data,write_data
from car_service import store_data
from time import sleep

app = Flask(__name__)

@app.route("/data/<key>",methods=['GET'])
def getdata(key):
    v = get_data(key)
    return f'db[{key}]={v}'
'''

@app.route("/data",methods=['POST'])
def postdata():
    key = request.form['key']
    value = request.form['value']
    write_data(key,value)
    return key
'''

# r stands for direction,from 0 to 2pi
data_keys = ['time_stamp','car_id','x','y','r','vx','vy','vr','image']
@app.route("/data",methods=['POST'])
def postdata():
    values = []
    for key in data_keys:
        values.append(request.form[key])
    geo_content = store_data(*values)
    return geo_content
    return f"{len(f'{values}')}"
