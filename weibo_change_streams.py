from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from kafka.producer import KafkaProducer
import json
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper
import argparse
from datetime import datetime
from pymongo import UpdateOne,DeleteOne


root_path = Helper().getRootPath()
logger = Logger(root_path + '/conf/logging.conf','logger_change_streams').createLogger()
apis = {'weibo':SocialWeiboAPI}
try:

    parser = argparse.ArgumentParser()
    parser.description = 'Mongodb DB-level Change Streams'
    parser.add_argument("-c","--collection",help="Weibo collection name",choices=['weibo_user_post','weibo_user_attitude','weibo_user_comment','weibo_user_repost'],required=True)
    args = parser.parse_args()
    opt = vars(args)

    __db = 'weibo'
    __api = apis[__db]()

    client = __api.client
    db = client[__db]
    col_stable = db[opt.get('collection')]
    col_tmp = opt.get('collection') + '_tmp_stream'
    col = db[col_tmp]
    pipeline = [{'$match': {"operationType": {'$in': ['insert', 'update', 'replace']}}}]

    logger.info("Start MongoDB Change Streams Service for table {}...".format(col_tmp))

    with col.watch(pipeline,full_document='updateLookup') as stream:

        for change in stream:
            update_operations = list()
            #delete_operations = list()
            logger.info(change)
            msg=str(change.get('documentKey').get('_id'))+','+str(change.get('clusterTime').time)
            topic= change.get('ns').get('coll')
            producer = KafkaProducer(bootstrap_servers=['172.16.42.3:9092'])
            producer.send(topic, key=bytes(json.dumps(change.get('ns')).encode('utf-8')),value=bytes(json.dumps(msg).encode('utf-8')), partition=0)
            producer.close()
            record = change.get('fullDocument')
            record.pop('_id')
            record.pop('createdTime')
            update_op = UpdateOne({'id':record['id']},{'$set':record,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},upsert=True)
            #delete_op = DeleteOne({'id':record['id']})
            update_operations.append(update_op)
            #delete_operations.append(delete_op)
            col_stable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            #col.bulk_write(delete_operations,ordered=False,bypass_document_validation=False)
except KeyboardInterrupt:
    pass
except Exception as e:
    print(e)
    logger.error(e)
finally:
    logger.info('Exit MongoDB Change Streams Service...')