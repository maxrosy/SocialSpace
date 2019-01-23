from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.Model import IdataAccount
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import *
import requests
import logging


logging.basicConfig(format='%(asctime)s %(levelname)s  %(message)s',level=logging.INFO,datefmt='%Y/%m/%d %H:%M:%S')
database ='idata'
collections=[]
username = 'idata_readonly'
pw = 'idata_readonly'
host = '127.0.0.1'

idata = IdataAPI()
session = idata.createSession()
accountInfo = session.query(IdataAccount.appCode, IdataAccount.type).filter(IdataAccount.status == 1).all()

collections = [account[0] + '_' + account[1] for account in accountInfo]
collections = ['idata_error_log']

session.close()

try:
    for collection in collections:
        index = 'idata_' + collection
        uri = "mongodb://{}:{}@{}/{}.{}".format(username,pw,host,database,collection)
        r = requests.delete('http://{}:9200/{}'.format(host,index))
        r = r.json()
        if r.get('acknowledged') == True:
             logging.info('Index {} has been purged'.format(index))
        else:
             reason = r.get('error').get('root_cause')[0].get('reason')
             ind = r.get('error').get('root_cause')[0].get('index')
             logging.warning('{} - {}'.format(reason,ind))

        spark = SparkSession.builder.master('local').getOrCreate()

        df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", uri).load()

        df = df.drop('_id')

        resource = index  + '/' + index

        df.write.format("org.elasticsearch.spark.sql").mode("append") \
              .option("es.resource", resource).save()

except Exception as e:
    logging.error(e)

finally:
    session()
    spark.stop()
