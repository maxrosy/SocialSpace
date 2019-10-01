#!/bin/sh
startTime=`date -d "yesterday" "+%Y-%m-%d 00:00:00"`
#startTime=`date -d "today" "+%Y-%m-%d 00:00:00"`
endTime=`date -d "today" "+%Y-%m-%d 00:00:00"`

err_num=0

sh /home/panther/data-integration/pan.sh -file=/home/panther/SocialSpace/jobs/mongo_to_mysql/zncrawlers_jobs/zncrawlers_hupu_search.ktr -param:startTime="$startTime" -param:endTime="$endTime"
if [ $? -ne 0 ];then
        let err_num++
fi

if [ $err_num -ne 0 ];then
	exit 1
fi


