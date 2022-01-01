from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
import pyodbc          
import pandas as pd
import os
from datetime import datetime   
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
import time
from datetime import timedelta
import sqlite3
from sqlite3 import Error

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PROPAGATE_EXCEPTIONS'] = True
app.config['SECRET_KEY'] = 'sdflksjslksajfsda' # NOT KOSSURE BUT FOR NOW

# Run app through sql alchemy and marshmallow 
db = SQLAlchemy(app)

# Create tables at first
@app.before_first_request
def create_tables():
    db.create_all() 


def update_and_schedule():
    os.remove("C:/Path_to_the_database_right_click_and_upload/data.db")  # Make this dynamic path to app.config with getwd
    print("db gone") # delete
    time.sleep(12) # delete
    db.create_all() 
    print("db back") # delete
    conn_str = ('DRIVER={SQL Server};'
    'SERVER=(Change_This);'
    'DATABASE=(Change_This);'
    'Trusted_Connection=yes;')
    cnxn = pyodbc.connect(conn_str)
    # get todays date
    df = "select top 10 dv.DIVISION, p.BUSINESS_UNIT, st.CUSTNAME, p.PRODUCT_GROUP_NAME, o.QTY, o.EXTENDED_AMT from hcs_discover.dw.factorders o left join HCS_DISCOVER.dw.DimProducts p on p.id = o.PRODUCT_ID left join HCS_DISCOVER.dw.DimShipTo st on st.id = o.SHIP_TO_ID left join HCS_DISCOVER.DW.DimDivision dv on dv.ID = o.DIV_ID where p.BUSINESS_UNIT IN('{}') and p.PRODUCT_GROUP_NAME IN('{}')".format('Orthopaedic Instruments','LB Cut Access')
    predata = pd.read_sql(df, cnxn)
    print(predata)
    # connect to sqlite3 to input into table
    connect = sqlite3.connect('data.db')
    predata.to_sql(name='table1', con=connect)
    #insert_values_to_table('data.db', predata)
    print("insterted data")

    

sched = BackgroundScheduler(daemon=True)
sched.add_job(update_and_schedule,'interval',seconds=40)
sched.start()

@app.route("/")
@app.route("/home")
def home():
    """ Function for test purposes. """
    return "Welcome Home :) !"

if __name__ == "__main__":
    #sched.start()
    app.run()