from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import *
import logging
import urllib

if __name__ == '__main__':

    try:
        logging.basicConfig(format='%(asctime)s %(levelname)s  %(message)s', level=logging.INFO,
                            datefmt='%Y/%m/%d %H:%M:%S')
        weibo = SocialWeiboAPI()
        mongod_db = 'weibo'
        mongo_table = 'weibo_search_statuses_history_result_merge'
        mongo_username = urllib.parse.quote_plus(weibo.cfp.get('mongodb_weibo','user'))
        mongo_pw = urllib.parse.quote_plus(weibo.cfp.get('mongodb_weibo','pwd'))
        host = '127.0.0.1'
        mysql_username = weibo.cfp.get('mysql','user')
        mysql_pw = weibo.cfp.get('mysql','password')
        mysql_db = weibo.cfp.get('mysql','db')
        mysql_table = 'weibo_search_history'


        mongo_uri = "mongodb://{}:{}@{}/{}.{}".format(mongo_username, mongo_pw, host, mongod_db, mongo_table)
        mysql_uri = "jdbc:mysql://{}:3306/{}?useUnicode=true&characterEncoding=utf8".format(host,mysql_db)

        spark = SparkSession.builder.master('local').getOrCreate()

        # Set up the schema
        fields = list()
        fields.append(StructField('mid', StringType(), True))
        fields.append(StructField('blog_url', StringType(), True))
        fields.append(StructField('content', StringType(), True))
        fields.append(StructField('created_at', StringType(), True))
        fields.append(StructField('extend',StringType(), True))
        fields.append(StructField('filter',StringType(), True))
        fields.append(StructField('huati_tag',StringType(), True))
        fields.append(StructField('mentions',StringType(), True))
        fields.append(StructField('parent_rt_id_db',StringType(), True))
        fields.append(StructField('source_id',StringType(), True))
        fields.append(StructField('uid',StringType(), True))
        fields.append(StructField('q',StringType(), True))
        schema = StructType(fields)

        # Read from mongodb
        history_records = spark.read.schema(schema)\
                        .format("com.mongodb.spark.sql.DefaultSource") \
                        .option("uri", mongo_uri).load()

        # Write to mysql
        history_records.write.format('jdbc') \
                        .option('url',mysql_uri)\
                        .option('user',mysql_username)\
                        .option('password',mysql_pw)\
                        .option('dbtable',mysql_table)\
                        .mode('append')\
                        .save()

    except Exception as e:
        print(e)
        exit(1)
    finally:
        spark.stop()