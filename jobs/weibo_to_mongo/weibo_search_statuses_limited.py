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
    date_time = datetime.datetime.now() + datetime.timedelta(hours=-24)
    start_time = date_time.strftime("%Y-%m-%d 00:00:00")
    end_time = date_time.strftime("%Y-%m-%d 23:59:59")

    for q in brand_queries:
        weibo.search_statuses_limited(start_time, end_time, q=q, hasori=1, dup=0, count=50)
