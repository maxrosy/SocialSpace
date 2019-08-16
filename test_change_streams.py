from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from kafka.producer import KafkaProducer
import json
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper

root_path = Helper().getRootPath()
logger = Logger(root_path + '/conf/logging.conf','logger_change_streams').createLogger()

weibo = IdataAPI()
db = weibo.client.idata
pipeline = [{ '$match': {"operationType" :{'$in': ['insert', 'update','replace']}}}]
try:
    logger.info("Start MongoDB Change Streams Service...")

    with db.watch(pipeline) as stream:
        batch_list = list()
        for change in stream:
            logger.info(change)
            msg=str(change.get('documentKey').get('_id'))
            topic = change.get('ns').get('db')+'_'+change.get('ns').get('coll')
            producer = KafkaProducer(bootstrap_servers=['172.16.42.3:9092'])
            producer.send(topic, key=bytes(json.dumps(change.get('ns')).encode('utf-8')),value=bytes(json.dumps(msg).encode('utf-8')), partition=0)
            producer.close()
            batch_list.clear()
except KeyboardInterrupt:
    pass
except Exception as e:
    logger.error(e)
finally:
    logger.info('Exit MongoDB Change Streams Service...')