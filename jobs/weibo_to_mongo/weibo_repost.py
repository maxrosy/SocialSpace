from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import pandas as pd
import numpy as np


if __name__ == '__main__':

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo.client
    db = client.weibo
    postTable = db.weibo_user_post
    repostTable = db.weibo_user_repost2

    startTime = weibo.getStrTime(-7)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]
    session.close()

    pidList = list(postTable.find({'uid': {'$in': uidList}, 'created_at_timestamp': {'$gte': startTimeStamp}}, {'id': 1,'reposts_count':1}))
    pList = [pid['id'] for pid in pidList]

    df_repostsInPost = pd.DataFrame(pidList)
    pipeline = [
        {'$match': {'pid': {'$in': pList}}},
        {'$group': {'_id': '$pid', 'since_id': {'$max': '$id'}, 'count': {'$sum': 1}}}
    ]
    repostList = list(repostTable.aggregate(pipeline))

    if repostList:
        df_repostsInRepost = pd.DataFrame(repostList)

        df = df_repostsInPost.merge(df_repostsInRepost, left_on='id', right_on='_id', how='left')

        df['since_id'] = df['since_id'].replace(np.nan, 0)
        df['count'] = df['count'].replace(np.nan, 0)
        df = df[df['reposts_count'] > df['count']]
    else:
        df = df_repostsInPost
        df['since_id']=0
    repostPostList = df[['id', 'since_id']].to_dict('records')
    weibo.doParallel('repost',repostPostList)
    weibo._client.close()

