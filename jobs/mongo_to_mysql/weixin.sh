#!/bin/sh

startTime=`date -d "yesterday" "+%Y-%m-%d 00:00:00"`
endTime=`date -d "yesterday" "+%Y-%m-%d 23:59:59"`

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weixin_post.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weixin_user_cumulate.ktr -param:startTime="$startTime" -param:endTime="$endTime"

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/weixin_upstream_msg.ktr -param:startTime="$startTime" -param:endTime="$endTime"
