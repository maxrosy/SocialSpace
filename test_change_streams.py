from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.SocialAPI.NewRankAPI import NewRankAPI
from kafka.producer import KafkaProducer
import json
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper
import argparse

root_path = Helper().getRootPath()
logger = Logger(root_path + '/conf/logging.conf','logger_change_streams').createLogger()
apis = {'idata':IdataAPI,'weibo':SocialWeiboAPI,'newrank':NewRankAPI}
try:

    parser = argparse.ArgumentParser()
    parser.description = 'Mongodb DB-level Change Streams'
    parser.add_argument("-d","--database",help="The name of the post type",choices=['idata','weibo','newrank'])
    args = parser.parse_args()
    opt = vars(args)

    __db = opt.get('database')
    __api = apis[__db]()

    client = __api.client
    db = client[__db]

    pipeline = [{'$match': {"operationType": {'$in': ['insert', 'update', 'replace']}}}]

    logger.info("Start MongoDB Change Streams Service for DB {}...".format(__db))

    with db.watch(pipeline) as stream:
        batch_list = list()
        for change in stream:
            logger.info(change)
            msg=str(change.get('documentKey').get('_id'))+','+str(change.get('clusterTime').time)
            topic = change.get('ns').get('db')+'_'+change.get('ns').get('coll')
            producer = KafkaProducer(bootstrap_servers=['172.16.42.3:9092'])
            producer.send(topic, key=bytes(json.dumps(change.get('ns')).encode('utf-8')),value=bytes(json.dumps(msg).encode('utf-8')), partition=0)
            producer.close()
            batch_list.clear()
except KeyboardInterrupt:
    pass
except Exception as e:
    print(e)
    logger.error(e)
finally:
    logger.info('Exit MongoDB Change Streams Service for DB {}...'.format(__db))