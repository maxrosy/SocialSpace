from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
import datetime
from SocialAPI.Model import MasterWeiboSearch

if __name__ =='__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    brands = session.query(MasterWeiboSearch.id,MasterWeiboSearch.search_query)\
        .filter(MasterWeiboSearch.status==1)\
        .all()
    brand_queries = brands#[brand[0] for brand in brands]
    session.close()

    # Get last hour time range
    # for x in range(30):
    start_date_time = datetime.datetime.now() + datetime.timedelta(days=-3)
    # end_date_time = datetime.datetime.now() + datetime.timedelta(hours=-72)
    # start_time = start_date_time.strftime("%Y-%m-%d 00:00:00")
    for q in brand_queries:
        for i in range(4):
            temptime = start_date_time.replace(hour=i*6, minute=0, second=0)
            start_time = temptime.strftime('%Y-%m-%d %H:%M:%S')

            temptime = start_date_time.replace(hour=(i*6)+5, minute=59, second=59)
            end_time = temptime.strftime('%Y-%m-%d %H:%M:%S')

            weibo.search_statuses_limited(start_time, end_time, q=q, hasori=1, dup=0, count=50, sort='hot', antispam=0)




