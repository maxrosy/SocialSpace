from kafka import KafkaConsumer,TopicPartition
from subprocess import check_call,CalledProcessError
import time
import json
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper
from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.SocialAPI.NewRankAPI import NewRankAPI
from znanalysis.Spider.HupuAPISail import HupuMongo
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from datetime import datetime

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
,'newrank_weixin_article_content'
,'newrank_weixin_search_content'
,'zncrawlers_hupu_search'
,'zncrawlers_douyin_post'
,'weibo_user_post_tmp_stream'
,'weibo_user_attitude_tmp_stream'
,'weibo_user_comment_tmp_stream'
,'weibo_user_repost_tmp_stream'
#,'idata_test'
)

consumer = KafkaConsumer(bootstrap_servers=['172.16.42.3:9092'],enable_auto_commit=False,group_id='socialdb-1')
consumer.subscribe(topics=topics)
apis = {'idata':IdataAPI,'newrank':NewRankAPI,'zncrawlers':HupuMongo,'weibo':SocialWeiboAPI}


def main():
    logger.info('Start Kafka Consumer Service...')
    tasks = {}
    while True:
        msg = consumer.poll(timeout_ms = 10000)
        if msg:
            try:
                for value in msg.values():
                    for v in value:
                        logger.info(v)
                        job = v.topic
                        last_offset = v.offset
                        updated_time = int(json.loads(v.value).split(',')[1])
                        if tasks.get(job):
                            if tasks[job]['start_time'] > updated_time:
                                tasks[job]['start_time'] = updated_time
                            if tasks[job]['end_time'] < updated_time:
                                tasks[job]['end_time'] = updated_time
                            if tasks[job]['last_offset'] < last_offset:
                                tasks[job]['last_offset'] = last_offset
                        else:
                            tasks[job] = {'start_time':updated_time,'end_time':updated_time,'last_offset':last_offset}
                for job,time_range in tasks.items():
                    start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_range['start_time']))
                    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_range['end_time']))
                    db_topic = job.split('_')[0]
                    if db_topic == 'idata_test':
                        continue
                    command = 'sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/{}_jobs/{}.ktr -param:startTime="{}" -param:endTime="{}"'\
                        .format(db_topic,job,start_time,end_time)
                    print(command)

                    result = check_call(command,shell=True)
                    if result==0:
                        __db = db_topic
                        __api = apis[__db]()
                        client = __api.client
                        db = client[__db]
                        col = db['kafka_offset']
                        record = {}
                        record['last_offset'] = time_range['last_offset']
                        record['topic'] = job
                        record['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        col.update({'topic': job}, {'$set': record, '$setOnInsert': {
                            'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, upsert=True)
                        #print('Done')

                consumer.commit()

            except CalledProcessError as e:
                logger.error(str(e))
            except KeyboardInterrupt:
                pass
            except Exception as e:
                logger.error(e)
                exit(1)
            time.sleep(2)


if __name__ == '__main__':
    main()