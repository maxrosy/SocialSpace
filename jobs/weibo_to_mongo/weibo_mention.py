from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import Kol, WeiboLastMentionedPost
import pandas as pd
import numpy as np
import threading

if __name__ =='__main__':
    weibo = SocialWeiboAPI()
    session = weibo.createSession()
    uid_list = session.query(WeiboLastMentionedPost.uid,WeiboLastMentionedPost.since_id).all()
    session.close()
    """
    client = weibo.client
    db = client.weibo
    mention_table = db.weibo_post_mention

    # get the latest mention post as the starting point
    pipeline = [
        {'$group': {'_id': '$uid_mentioned', 'since_id': {'$max': '$id'}, 'count': {'$sum': 1}}}
    ]
    mention_list = list(mention_table.aggregate(pipeline))
    if mention_list:
        df_mention_list = pd.DataFrame(mention_list)
        df_mention_list['_id'] = pd.to_numeric(df_mention_list['_id'])
        df = df_uid_list.merge(df_mention_list,left_on='uid',right_on='_id',how='left')
        df['since_id'] = df['since_id'].replace(np.nan, 0)

    else:
        df = df_uid_list
        df['since_id'] = 0

    uid_mention_list = df[['uid', 'since_id']].to_dict('records')
    
    #uid_mention_list = [{'uid':1006421732,'since_id':4325734551722946}]
    """
    uid_mention_list = [{'uid':_[0], 'since_id':_[1]} for _ in uid_list]
    weibo.doParallel('mention', uid_mention_list)

    #for item in uid_mention_list:
    #    weibo.get_statuses_mentions_other(item['uid'],page_limit=10,since_id=int(item['since_id']),filter_by_type=1,count=200)

    """
    threads = []

    for item in uid_mention_list:
        t = threading.Thread(target=weibo.get_statuses_mentions_other, args=(item['uid'],), kwargs={'pageLimit':10,'since_id': int(item['since_id']), 'count': 200})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) < 50:
                break
    """