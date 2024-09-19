from sqlalchemy import create_engine
import pymysql
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from sqlalchemy import text
from fastapi import FastAPI

host = 'localhost'
port = 3306
user = 'root'
passw = 'admin'
database = 'kafka_sql'

conn = pymysql.connect(host=host,
                       port=port,
                       user=user,
                       passwd=passw,
                       db=database,
                       charset='utf8')

mycursor = conn.cursor()
engine = create_engine("mysql+mysqlconnector://root:admin@localhost/kafka_sql")
connection = engine.connect()

app = FastAPI()


@app.get("/")
async def root():
    res = connection.execute(text(f'SELECT * FROM orders'))
    rows = []
    for row in res.mappings():
        rows.append(row)
    return str('\n'.join(str(v) for v in rows))

@app.get("/{id_inp}")
async def find_ids(id_inp:int):
    res = connection.execute(text(f'SELECT * FROM orders WHERE user_id = {id_inp}'))
    rows = []
    for row in res.mappings():
        rows.append(row)
    return str('\n'.join(str(v) for v in rows))
