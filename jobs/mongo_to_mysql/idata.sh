#!/bin/sh

startTime=`date -d "yesterday" "+%Y-%m-%d 00:00:00"`
endTime=`date -d "today" "+%Y-%m-%d 23:59:59"`

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_idataapi_article.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_baidutieba_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_douyin_video.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_hupu_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_iqiyi_video.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_qqsport_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_tencent_video.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_xiaohongshu_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_zhihu_question.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_zhihu_answer.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_weixinpro_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_weixinpro_post_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_baidutieba_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_hupu_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_douyin_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_iqiyi_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_qqsport_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_tencent_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_xiaohongshu_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_zhihu_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_bilibili_video.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_bilibili_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_dongqiudi_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_dongqiudi_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_baidutieba_reply.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_toutiao_news.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_toutiao_video.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_toutiao_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"
#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_taobao_product.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_taobao_image_urls.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_taobao_keyvalues.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_tmall_product.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_tmall_image_urls.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_tmall_keyvalues.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_pinduoduo_product.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_pinduoduo_image_urls.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/idata_jobs/idata_pinduoduo_keyvalues.ktr -param:startTime="$startTime" -param:endTime="$endTime"
