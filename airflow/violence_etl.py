# Import necessary libraries
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

service_account_key = {
    "type": "service_account",
    "project_id": "distributed-system-347306",
    "private_key_id": "0a1067247f10b6f020cd6049fef8160b8a77d419",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC1SU42UnF/4kML\nlHcBt9bj6wdafeaAELmz2MfoWf2alipd83wFtjrd330SdYuX2B+c0mIjM6JT6Fk6\n1U7rroKh5K+uvubixGT2LEdq6gZQ1vttlk66Kf0MlDvO1zP7Ugc6THAY5+G119nI\n6o4vEDHgY3zI21tjNwnch3UtwnKJbG0LCEL63Z/eO7HmV8tHFfJv+BwQ2mkYI2Hb\n3Lb/6vMP6Mlsb3ME807cQSzmJ3H/+aiWE/tYPSdY11E+oRrmT/r0rtlTZGkYgWCt\nrlDSVNa5GxjvKWEi1gKa6DKdw3vOybj9fcPPvUIVgI5ufX3EO3za4yCAa5H/lFp8\n+3UwkHx5AgMBAAECggEAB1FLD0mG5BH4oZcnshmu+oJ2SKZwF0+FhW9My3HVeSfS\nVDpDsXHUgLf4kxYMUU/jn8eEt1XKWb91WZj6X2xLANirqD6zsGfbyO2K4POLF3iE\nC91/NkfDW36AxwW8BwsduFJVTIYQn+/Tbkccf+UUFFIDu5JSJyg1tVGA7vzMNYSJ\n0D5HC99eC9DEzfp+KWWiC8Asl2hh/gzDFcmkdoSRWoN7QIrzYwBInX52AmGXmiog\n1tzAfbi8eaUntx3Ce2+sQ4KatUPqJv5b69QkFeAJwced4m4TXirNWvkEoBJECZrv\ntjoLtGtotypzD8al6/U+YSy+UQ1bNYzP1EWxNh8tkQKBgQDr2AZt22nEuuV2TVt9\ng1vRBd7SeL2ljx4A+bMEWABmI9gDJHf5N8gRgqmAn4snHrWM83VoOEAmJUaZcwol\ndNs016hk7fGhrGT53cRO01V6gW5GOS1H0BS+Yz1dCCiEVC0tNNU8ahwW6Re7/16A\nsDLSZwdqx2UqVdxwWmYv0s/2yQKBgQDEx6ELPZTW03gLEP295gEkm2tgyZobLncu\noSJCJ37zo3EGiSv+btwLTcEwJ+x7e73GZ1NDcoNtVQk5vBWThs2/xOb7gOhm7LVo\nTlqJ3QSxso1kcizX8SGTyU9yfhy6c7sn71erfAV8EXRT+o/wf+hDQJ4Hu7GYPWQ3\nyXpCyJBAMQKBgQC9OiPWaxCPB0HqzKCWsRmvOR2SE8xeFNmHANHQr2cKjMHZq54T\nON1upz6m89urdKlIQWK3T0KxGIFvx2yhpwPmfw4ehQe1p2ORU40ZjdjspQK8l02s\na9jo9SkcBtqzafKhbd2VTrHg8/7WGUxSxozQYlgCJaAW+rMW9oE859FyeQKBgQCj\n20dywLirmjOfo2pnMptJDFIBql40vCBqJ6sLQzAnWLXJJ3gGyfFZSEnR+6yjtop0\nJ53hz/04kVK5TLD6w4mYYjGkw9bBveHhFT23Bt/dyDyMo7ZLqnK3SS5qvDDX4X66\n5kYueXtnsHEZbM9nCFszhR8SeWyMxLIq8g5ohfH4QQKBgQClHYpv4Spu/a/irNMj\n4OBJxzR6HR4bXJlM56sxpoo5gaW79WRLpsTZmdyna/AA6gXKXI95uckzDVZWhWSb\njiX71SOua0JHyieVYdeBQ4lwi+mNPhG/NJcq6Hw4a8RJpxfXIhxkkYqwY19XWObb\nEq9W7FTu0JTUEm8d1NOzaWmirg==\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk-dof1b@distributed-system-347306.iam.gserviceaccount.com",
    "client_id": "113310019367947301225",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-dof1b%40distributed-system-347306.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}


class MongoDB:
    def __init__(self, uri, dbname, collection_name):
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        self.db = self.client[dbname]
        self.collection = self.db[collection_name]

    def get_all_documents(self):
        return list(self.collection.find())

    def delete_all_documents(self):
        self.collection.delete_many({})

    def get_item_by_field(self, field, value):
        return list(self.collection.find({field: value}))

    def add_document(self, document):
        self.collection.insert_one(document)

    def count_documents(self):
        return self.collection.count_documents({})


class FirestoreData:

    def __init__(self) -> None:
        self.__cred = credentials.Certificate(service_account_key)
        if not firebase_admin._apps:
            self.fs = firebase_admin.initialize_app(self.__cred)
        else:
            self.fs = firebase_admin.get_app()

        self.db = firestore.client()
        # self.collection = self.db.collection(index)

    def get_all(self, collection_name):
        docs = self.db.collection(collection_name).get()
        return docs

    def delete_all_collection(self, collection_name):
        return self.db.collection(collection_name).delete()

    def get_item_by_key_value(self, collection_name, key, value):
        doc = self.db.collection(collection_name).where(key, '==', value).get()
        return doc

    def add_document(self, data, collection_name):
        doc_ref = self.db.collection(collection_name).document()
        doc_ref.set(data)

    def count_all(self, collection_name):
        return len(self.db.collection(collection_name).get())


# Firestore
DATA_INDEX = 'violence-data'
RP_INDEX = 'violence-report'
# es_tract = FirestoreData(DATA_INDEX)
fs = FirestoreData()

# Create SparkSession
spark = SparkSession.builder \
    .appName("PySpark ETL with GCP") \
    .getOrCreate()


def extract():
    # Define schema for the incoming dataframe
    schema = StructType([
        StructField("predicted_class_name", StringType()),
        StructField("predicted_labels_probabilities", DoubleType()),
        StructField("saved_filename", StringType()),
        StructField("timestamp", StringType()),
        StructField('location', StructType([
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType())
        ]))
    ])
    df = []
    # Load data from Data Source
    for hit in fs.get_all(DATA_INDEX):
        df.append(hit.to_dict())
    df = spark.createDataFrame(data=df, schema=schema)
    return df


def transform(df):
    data = df.withColumn("datetime", to_timestamp("timestamp", "yyyy/MM/dd HH:mm:ss"))
    # df2.printSchema()
    data = data.na.drop(how='any')
    # Extract the relevant information and group by date, month, longitude, and latitude

    processed_df = data.select(
        to_date("datetime").alias("date"),
        month("datetime").alias("month"),
        col("location.lon").alias("longitude"),
        col("location.lat").alias("latitude"),
        col("predicted_class_name").alias("violent_incident"),
        col("predicted_labels_probabilities").alias("probabilities")) \
        .groupBy("date", "month", "longitude", "latitude") \
        .agg(count("violent_incident").alias("total_violence"),
             round(mean("probabilities"), 4).alias("avg_probabilities"))

    # Show the processed data
    processed_df.orderBy(col('date').asc(), col("total_violence").desc()).show(truncate=False)
    return processed_df.withColumn("date", processed_df["date"].cast(StringType()))


def load(df_transformed):
    for row in df_transformed.collect():
        fs.add_document(collection_name=RP_INDEX, data=row.asDict(True))

if __name__ == '__main__':
    df = extract()
    df_transformed = transform(df)
    load(df_transformed=df_transformed)
