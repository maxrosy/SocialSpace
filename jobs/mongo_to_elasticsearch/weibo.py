
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import *
import requests
import logging

"""
sc = SparkContext()
ctx = SQLContext(sc)



uri = "mongodb://weibo_readonly:weibo_readonly@127.0.0.1:27017"
database = 'weibo'
collection = 'weibo_user_post'
df = ctx.read.format("com.mongodb.spark.sql").options(uri=uri, database=database, collection=collection).load()
print("Done")
"""
logging.basicConfig(format='%(asctime)s %(levelname)s  %(message)s',level=logging.INFO,datefmt='%Y/%m/%d %H:%M:%S')
database ='weibo'
collections=['weibo_user_growth','weibo_user_post','weibo_post_crawl']
username = 'weibo_readonly'
pw = 'weibo_readonly'
host = '127.0.0.1'

for index in collections:
     uri = "mongodb://{}:{}@{}/{}.{}".format(username,pw,'127.0.0.1',database,index)
     r = requests.delete('http://{}:9200/{}'.format(host,index))
     r = r.json()
     if r.get('acknowledged') == True:
          logging.info('Index {} has been purged'.format(index))
     else:
          reason = r.get('error').get('root_cause')[0].get('reason')
          ind = r.get('error').get('root_cause')[0].get('index')
          logging.warning('{} - {}'.format(reason,ind))

     spark = SparkSession.builder.master('local').getOrCreate()


     if index == 'weibo_user_post':

          fields_list = "id attitudes_count comments_count created_at uid reposts_count source text"
          fields = [StructField(field_name, StringType(), True) for field_name in fields_list.split()]
          schema = StructType(fields)

          df = spark.read.schema(schema).format("com.mongodb.spark.sql.DefaultSource")\
              .option("uri", uri).load()
     else:
          df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", uri).load()

     df = df.drop('_id')

     resource = index + '/' + index

     df.write.format("org.elasticsearch.spark.sql").mode("append") \
          .option("es.resource", resource).save()


