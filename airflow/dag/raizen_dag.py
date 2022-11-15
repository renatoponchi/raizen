from airflow import DAG, Dataset
from datetime import datetime, date
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sqlalchemy as sa
import pandas as pd
import json
import numpy as np
import os


def read_file():
    
    df = pd.read_csv("data/2000.csv", sep=';').astype(str)

#    df_union_all = pd.concat([df_2000,df_2001,df_2002,df_2003,df_2004,df_2005,df_2006,df_2007,df_2008,df_2009,
#                              df_2010,df_2011,df_2012,df_2013,df_2014,df_2015,df_2016,df_2017,df_2018,df_2019,
#                              df_2020])
    
    df_json = df.to_json(orient="records")

    return df_json

def raw_truncate():
    print("TRUNCATE TABLE raizen.petroleum.raw")

def raw_insert(ti):
    print("INSERT raizen.petroleum.raw")
    #data_origem = ti.xcom_pull(task_ids = 'read_file')
    #data_origem = json.loads(data_origem)
    #data_origem = json.dumps(data_origem)
    #data_origem = pd.read_json(data_origem)

    # Conecta com BD
    #engine = sa.create_engine('postgresql://postgres:airflow@host.docker.internal:5432/airflow')

    # Insert
    #insere = data_origem.to_sql('petroleum.raw', engine, if_exists='append', index=False)

def trusted():
    print('trusted')

def delivery():
    print('delivery')

# DAG AirFlow
#pega a data e a hora de hoje
today = datetime.today()
dataset = Dataset('petroleum/petroleum.sales')

# essa dag roda todas as vezes que o arquivo for alterado
with DAG('raizen_dag', start_date= today,  schedule_interval='30 * * * *',
         catchup=False) as dag:

        read_file = PythonOperator(
            task_id = 'read_file',
            python_callable = read_file
        )

        raw_truncate = PostgresOperator(
            task_id = 'raw_truncate',
            postgres_conn_id = 'postgres_localhost',
            sql = """
            TRUNCATE TABLE raizen.petroleum.raw
            """
        )

        raw_insert = PythonOperator(
            task_id = 'raw_insert',
            python_callable= raw_insert
        )

        trusted = PythonOperator(
            task_id = 'trusted',
            python_callable = trusted
        )

        delivery = PythonOperator(
            task_id = 'delivery',
            python_callable = delivery
        )

        read_file >> raw_truncate >> raw_insert >> trusted >> delivery