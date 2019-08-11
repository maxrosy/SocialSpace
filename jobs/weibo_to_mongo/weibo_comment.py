from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import WeiboKolLastComment,WeiboSearchLimitedLastComment,WeiboMentionLastComment

if __name__ == '__main__':
    # Get the last 2000 comments for each post at most
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()


        startTime = weibo.getStrTime(-7)
        # Get post IDs from search limited for comment
        pids = session.query(WeiboSearchLimitedLastComment.pid, WeiboSearchLimitedLastComment.since_id) \
            .filter(WeiboSearchLimitedLastComment.created_at >= startTime) \
            .all()
        commentPostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Get post IDs from post daily for comment
        pids = session.query(WeiboKolLastComment.pid, WeiboKolLastComment.since_id) \
            .filter(WeiboKolLastComment.created_at >= startTime) \
            .all()
        commentPostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Get post IDs from mention for comment
        pids = session.query(WeiboMentionLastComment.pid, WeiboMentionLastComment.since_id) \
            .filter(WeiboMentionLastComment.created_at >= startTime) \
            .all()
        commentPostListFromMetion = [{'id': _[0], 'since_id': _[1]} for _ in pids]

        # Merge daily, kol, mention pid
        commentPostList += commentPostListFromKOL
        commentPostList += commentPostListFromMetion

        weibo.doParallel('comment',commentPostList)
    finally:
        session.close()