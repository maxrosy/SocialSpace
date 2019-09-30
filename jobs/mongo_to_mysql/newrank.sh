#!/bin/sh
startTime=`date -d "yesterday" "+%Y-%m-%d 00:00:00"`
#startTime=`date -d "today" "+%Y-%m-%d 00:00:00"`
endTime=`date -d "today" "+%Y-%m-%d 23:59:59"`

err_num=0

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/newrank_jobs/newrank_weixin_article_content.ktr -param:startTime="$startTime" -param:endTime="$endTime"
if [ $? -ne 0 ];then
        let err_num++
fi

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/newrank_jobs/newrank_weixin_search_content.ktr -param:startTime="$startTime" -param:endTime="$endTime"
if [ $? -ne 0 ];then
        let err_num++
fi

if [ $err_num -ne 0 ];then
	exit 1
fi
