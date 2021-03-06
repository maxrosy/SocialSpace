import pandas as pd
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Crawler import WeiBoCrawler
from SocialAPI.Model import Kol
import time
from datetime import datetime
from pymongo import MongoClient

if __name__ == '__main__':
    myHelper = Helper()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo.client
    db = client.weibo
    crawlTable = db.weibo_post_crawl
    postTable = db.weibo_user_post

    crawlDict = {}
    startTime = weibo.getStrTime(-30)
    startTimeStamp = weibo.getTimeStamp(startTime)
    userDict = {}
    userInfo = session.query(Kol.uid,Kol.username,Kol.pw).filter(Kol.status == 1, Kol.crawl_status==1).all()

    #userInfo = session.query(Kol.uid,Kol.username,Kol.pw).filter(Kol.uid==2036201132).all()

    for user in userInfo:
        userDict[user[0]] = (user[1],user[2])

    for uid in userDict.keys():
        pids = postTable.find({'uid':uid,'created_at_timestamp':{'$gte':startTimeStamp}},{'id':1})
        pids = [pid['id'] for pid in pids]
        crawlDict[uid] = pids

    result = []
    weiboCrawler = WeiBoCrawler()

    for uid, pids in crawlDict.items():
        #udf = df[df['uid']==uid].reset_index()

        weiboCrawler.login(userDict[uid][0],userDict[uid][1])

        for pid in pids:
        #for pid in [4312910043806040]:
            mid = myHelper.convertIdtoMid(pid)
            url = 'https://weibo.com/'+ str(uid) + '/' + str(mid)
            html = weiboCrawler.crawlPage(url)
            impressions = weiboCrawler.getImpressions(html)
            forwards = weiboCrawler.getForwards(html)
            comments = weiboCrawler.getComments(html)
            likes = weiboCrawler.getLikes(html)

            result.append({'pid':pid,'uid':uid, 'impression':impressions,'forward':forwards,'comment':comments,'like':likes,'url':url,
                           'crawlDate':time.strftime("%Y-%m-%d", time.localtime())})

    for post in result:
        post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')#int(time.time())
        res = crawlTable.update_one({'pid':post['pid'],'crawlDate':time.strftime("%Y-%m-%d", time.localtime())},
                                {'$set': post, '$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, upsert=True)
    client.close()
    session.close()


