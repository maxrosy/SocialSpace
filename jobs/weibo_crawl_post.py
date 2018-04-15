import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Crawler import WeiBoCrawler
from SocialAPI.Model import PostStatus, PostCrawl
import time

if __name__ == '__main__':
    myHelper = Helper()
    rootPath = myHelper.getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    df.drop_duplicates(inplace=True)

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    crawlDict = {}

    for uid in df['uid']:
        pids = session.query(PostStatus.id).filter(PostStatus.uid == uid).order_by(PostStatus.created_at.desc()).limit(2).all()
        pids = [pid[0] for pid in pids]
        crawlDict[uid] = pids

    result = []
    weiboCrawler = WeiBoCrawler()

    for uid, pids in crawlDict.items():
        udf = df[df['uid']==uid].reset_index()

        weiboCrawler.login(udf['username'][0], udf['pw'][0])

        for pid in pids:
            mid = myHelper.convertIdtoMid(pid)
            url = 'https://weibo.com/'+ str(uid) + '/' + str(mid)
            html = weiboCrawler.crawlPage(url)
            impressions = weiboCrawler.getImpressions(html)
            forwards = weiboCrawler.getForwards(html)
            comments = weiboCrawler.getComments(html)
            likes = weiboCrawler.getLikes(html)

            result.append({'pid':pid,'uid':uid, 'impression':impressions,'forward':forwards,'comment':comments,'like':likes,
                           'update_date':time.strftime("%Y-%m-%d", time.localtime())})
    resultDf = pd.DataFrame(result)
    weibo.upsertToDB(PostCrawl, resultDf)




