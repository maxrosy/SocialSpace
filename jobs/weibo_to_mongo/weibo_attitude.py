from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol, MonsterWeiboPost
import pandas as pd
import numpy as np


if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    rootPath = Helper().getRootPath()

    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    client = weibo.client
    db = client.weibo
    postTable = db.weibo_user_post
    attitudeTable = db.weibo_user_attitude


    startTime = weibo.getStrTime(-7)
    startTimeStamp = weibo.getTimeStamp(startTime)
    uids = session.query(Kol.uid).all()
    uidList = [uid[0] for uid in uids]

    session.close()

    pidList = list(postTable.find({'uid':{'$in':uidList},'created_at_timestamp':{'$gte':startTimeStamp}},{'id':1,'attitudes_count':1}))
    pList = [pid['id'] for pid in pidList]
    df_attitudesInPost = pd.DataFrame(pidList)
    
    """
    ########
    # For monster
    pids = session.query(MonsterWeiboPost.post_id).all()
    pList = [pid[0] for pid in pids]
    #pList = [4147480004230641]
    df_attitudesInPost = pd.DataFrame({'id':pList})
    ########
    """
    pipeline = [
        {'$match': {'status.id': {'$in': pList}}},
        {'$group': {'_id': '$status.id', 'since_id': {'$max': '$id'},'count':{'$sum':1}}}
    ]
    attitudeList = list(attitudeTable.aggregate(pipeline))

    if attitudeList:
        df_attitudesInAttitude = pd.DataFrame(attitudeList)

        df = df_attitudesInPost.merge(df_attitudesInAttitude,left_on='id',right_on='_id',how='left')

        df['since_id'] = df['since_id'].replace(np.nan,0)
        df['count'] = df['count'].replace(np.nan,0)
        #df = df[df['attitudes_count']>df['count']]

        attitudePostList = df[['id','since_id']].to_dict('records')
    else:
        df = df_attitudesInPost
        df['since_id'] = 0
        attitudePostList = df.to_dict('records')
    weibo.doParallel('attitude',attitudePostList)
    #weibo._client.close()


