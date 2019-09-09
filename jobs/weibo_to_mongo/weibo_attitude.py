from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import WeiboSearchLimitedLastAttitude, WeiboKolLastAttitude,WeiboMentionLastAttitude
import pandas as pd

def main(day_back):
    # Get the last 2000 comments for each post at most
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()

        startTime = weibo.getStrTime(day_back)
        # Get post IDs from search limited for attitude
        pids = session.query(WeiboSearchLimitedLastAttitude.pid,WeiboSearchLimitedLastAttitude.since_id)\
            .filter(WeiboSearchLimitedLastAttitude.created_at>=startTime)\
            .all()
        attitudePostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Get post IDs from post daily for attitude
        pids = session.query(WeiboKolLastAttitude.pid,WeiboKolLastAttitude.since_id)\
            .filter(WeiboKolLastAttitude.created_at>=startTime)\
            .all()
        attitudePostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Get post IDs from mention for attitude
        pids = session.query(WeiboMentionLastAttitude.pid, WeiboMentionLastAttitude.since_id) \
            .filter(WeiboMentionLastAttitude.created_at >= startTime) \
            .all()
        attitudePostListFromMention = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Merge daily, kol, mention pid
        attitudePostList += attitudePostListFromKOL
        attitudePostList += attitudePostListFromMention
        if attitudePostList:
            df = pd.DataFrame(attitudePostList)
            df = df.sort_values(by='since_id', ascending=False)
            df = df.drop_duplicates(subset='id', keep='first')
            attitudePostList_dedup = df.to_dict('records')
            weibo.doParallel('attitude',attitudePostList_dedup)
    finally:
        session.close()

if __name__ == '__main__':
    main(-7)


