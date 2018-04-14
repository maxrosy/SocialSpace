import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Crawler2 import Launcher,ParseVolume
from SocialAPI.Model import PostStatus, PostCrawl
import urllib

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
        mids = [myHelper.convertMidtoId(pid[0]) for pid in pids]
        crawlDict[uid] = mids
    a=1
    result = []

    for uid, mids in crawlDict.items():
        udf = df[df['uid']==uid].reset_index()

        launcher = Launcher(udf['username'][0], udf['pw'][0])
        launcher.login()
        for mid in mids:
            url = 'https://weibo.com/'+ str(uid) + '/' + str(mid)
            request = urllib.request.Request(url)
            response = urllib.request.urlopen(request)
            content = response.read().decode('utf-8')
            parser = ParseVolume(content)

            impressions = parser.getImpressions()
            forwards = parser.getForwards()
            comments = parser.getComments()
            likes = parser.getLikes()
            result.append({'mid':mid,'uid':uid, 'impression':impressions,'forward':forwards,'comment':comments,'like':likes})
    resultDf = pd.DataFrame(result)

    a=2

