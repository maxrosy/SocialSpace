#!/bin/sh

startTime=`date -d "today" "+%Y-%m-%d 00:00:00"`
endTime=`date -d "today" "+%Y-%m-%d 23:59:59"`

startTime_yesterday=`date -d "yesterday" "+%Y-%m-%d 00:00:00"`
endTime_yesterday=`date -d "yesterday" "+%Y-%m-%d 23:59:59"`

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_user_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_user_growth.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_post_media.ktr -param:startTime="$startTime" -param:endTime="$endTime"

#sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_post_crawl.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_post_mention.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_user_info.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_post_mention_media.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_search_statuses_limited_media.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_search_statuses_limited.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_user_comment.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weibo_jobs/weibo_user_attitude.ktr -param:startTime="$startTime" -param:endTime="$endTime"
