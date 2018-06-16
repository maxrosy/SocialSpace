import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Crawler import WeiBoCrawler
from SocialAPI.Model import PostStatus, PostCrawl
from SocialAPI.Model import Kol
import time
import os
from pymongo import MongoClient

if __name__ == '__main__':
    myHelper = Helper()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()

    client = MongoClient()
    db = client.weibo
    crawlTable = db.weibo_post_crawl

    crawlDict = {}
    startTime = weibo.getStrTime(-7)
    userDict = {}
    userInfo = session.query(Kol.uid,Kol.username,Kol.pw).filter(Kol.status == 1, Kol.crawl_status==1).all()
    #uidList = [uid[0] for uid in uids]
    #for user in userInfo:
    #    userDict[user[0]] = (user[1],user[2])
    userDict[5933632405] = ('cnpogba@sina.cn','Adidas01!')
    userDict[5210739467] =('jamestwitter@gmail.com','zhehenadi2016')
    for uid in userDict.keys():
        pids = session.query(PostStatus.id).filter(PostStatus.uid == uid, PostStatus.created_at >= startTime).all()
        #pids = session.query(PostStatus.id).filter(PostStatus.uid == uid).order_by(PostStatus.created_at.desc()).limit(10).all()
        pids = [pid[0] for pid in pids]
        crawlDict[uid] = pids

    result = []
    weiboCrawler = WeiBoCrawler()

    for uid, pids in crawlDict.items():
        #udf = df[df['uid']==uid].reset_index()

        weiboCrawler.login(userDict[uid][0],userDict[uid][1])

        for pid in pids:
            mid = myHelper.convertIdtoMid(pid)
            url = 'https://weibo.com/'+ str(uid) + '/' + str(mid)
            html = weiboCrawler.crawlPage(url)
            impressions = weiboCrawler.getImpressions(html)
            forwards = weiboCrawler.getForwards(html)
            comments = weiboCrawler.getComments(html)
            likes = weiboCrawler.getLikes(html)

            result.append({'pid':pid,'uid':uid, 'impression':impressions,'forward':forwards,'comment':comments,'like':likes,
                           'crawlDate':time.strftime("%Y-%m-%d", time.localtime())})

    for post in result:
        post['updatedTime'] = int(time.time())
        res = crawlTable.update({'pid':post['pid'],'crawlDate':time.strftime("%Y-%m-%d", time.localtime())},
                                {'$set': post, '$setOnInsert': {'createdTime': int(time.time())}}, upsert=True)
    client.close()
    session.close()


