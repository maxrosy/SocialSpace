from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import WeiboSearchLimitedLastAttitude, WeiboKolLastAttitude,WeiboMentionLastAttitude

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()

        startTime = weibo.getStrTime(-7)
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

        weibo.doParallel('attitude',attitudePostList)
    finally:
        session.close()



