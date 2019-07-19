from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
import datetime
from SocialAPI.Model import WeiboBrandSearch

if __name__ =='__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    brands = session.query(WeiboBrandSearch.id,WeiboBrandSearch.search_query).all()
    brand_queries = brands#[brand[0] for brand in brands]
    session.close()


    # Get last hour time range
    date_time = datetime.datetime.now() + datetime.timedelta(hours=-1)
    start_time = date_time.strftime("%Y-%m-%d %H:00:00")
    end_time = date_time.strftime("%Y-%m-%d %H:59:59")

    for q in brand_queries:
        weibo.search_statuses_limited(start_time, end_time, q=q, hasori=1, dup=0, count=50)
