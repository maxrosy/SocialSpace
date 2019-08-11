from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import WeiboSearchLimitedLastRepost,WeiboKolLastRepost


if __name__ == '__main__':

    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()


        startTime = weibo.getStrTime(-7)
        startTimeStamp = weibo.getTimeStamp(startTime)
        # Get post IDs from search limited for repost
        pids = session.query(WeiboSearchLimitedLastRepost.pid, WeiboSearchLimitedLastRepost.since_id) \
            .filter(WeiboSearchLimitedLastRepost.created_at >= startTime) \
            .all()

        repostPostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Get post IDs from post daily for repost
        pids = session.query(WeiboKolLastRepost.pid, WeiboKolLastRepost.since_id) \
            .filter(WeiboKolLastRepost.created_at >= startTime) \
            .all()

        repostPostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]
        repostPostList += repostPostListFromKOL

        weibo.doParallel('repost',repostPostList)
    finally:
        session.close()
