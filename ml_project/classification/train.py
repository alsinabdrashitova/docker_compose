from __future__ import print_function
import os
import mlflow
import pandas as pd
import psycopg2
import time

from mlflow import log_param, active_run

os.environ["AWS_DEFAULT_REGION"] = "eu-west-3"
os.environ["AWS_REGION"] = "eu-west-3"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "adminadmin"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"


def db_connection():
    con = psycopg2.connect(
        database="db",
        user="db",
        password="123456",
        host="postgres",
        port="5432"
    )

    cur = con.cursor()
    for i in range(1000):
        try:
            cur.execute("SELECT * from data_markup")
            return cur.fetchall()
        except:
            print('База данных еще не готова')
            time.sleep(1)


if __name__ == "__main__":

    data = pd.DataFrame(data=db_connection(), columns=['ind', 'items.name', 'clusters', 'items.price', 'items.quantity' ])
    data = data.iloc[:, 1:]

    print(data)

    if not os.path.exists("outputs"):
        os.makedirs("outputs")
    data.to_csv('outputs/data.csv')

    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment('classification')

    with mlflow.start_run() as run:
        log_param("noiseLimit", 766)

        mlflow.log_artifact('outputs/data.csv')

        print("Model saved in run %s" % active_run().info.run_uuid)
        print("Success")