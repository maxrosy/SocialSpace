from kafka import KafkaConsumer
from subprocess import call,check_output
import time
import json
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper

root_path = Helper().getRootPath()
logger = Logger(root_path + '/conf/logging.conf','logger_kafka').createLogger()
topics = (
'idata_baidutieba_comment'
,'idata_baidutieba_post'
,'idata_baidutieba_reply'
,'idata_bilibili_comment'
,'idata_bilibili_video'
,'idata_dongqiudi_comment'
,'idata_dongqiudi_post'
,'idata_douyin_comment'
,'idata_douyin_video'
,'idata_hupu_comment'
,'idata_hupu_post'
,'idata_idataapi_article'
,'idata_iqiyi_comment'
,'idata_iqiyi_video'
,'idata_qqsport_comment'
,'idata_qqsport_post'
,'idata_tencent_comment'
,'idata_tencent_video'
,'idata_toutiao_comment'
,'idata_toutiao_news'
,'idata_toutiao_video'
,'idata_xiaohongshu_comment'
,'idata_xiaohongshu_post'
,'idata_zhihu_answer'
,'idata_zhihu_comment'
,'idata_zhihu_question'
,'newrank_weixin_article_search'
,'newrank_weixin_search_content'
,'idata_test'
)

consumer = KafkaConsumer(bootstrap_servers=['172.16.42.3:9092'])
consumer.subscribe(topics=topics)
tasks = {}

logger.info('Start Kafka Consumer Service...')

while True:

    msg = consumer.poll(timeout_ms = 1000)
    if msg:
        try:
            for value in msg.values():
                for v in value:
                    logger.info(v)
                    db_topic = json.loads(v.key).get('db')
                    job = v.topic
                    _id = '{{$oid:\"{}\"}}'.format(json.loads(v.value))
                    if tasks.get(job):
                        tasks[job] += [_id]
                    else:
                        tasks[job] = [_id]
            for job,_ids in tasks.items():
                # pass less arguments to command
                for _ in range(0, len(_ids), 20):
                    ids = ','.join(_ids[_:_ + 20])
                    ids = '\'['+ids+']\''
                    command = 'sh /home/panther/data-integration/pan.sh \
                            -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/{}_jobs/{}.ktr \
                            -param:ids={}'.format(db_topic,job,ids)
                    print(command)

                    result = call(command,shell=True)
                    if result==0:
                        print('Done')

        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(e)
        time.sleep(2)

"""
for msg in consumer:
    recv = "{}:{}:{}: key={} value={}".format(msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print(recv)
"""