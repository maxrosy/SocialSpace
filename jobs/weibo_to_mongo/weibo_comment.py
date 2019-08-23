from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
from SocialAPI.Model import WeiboKolLastComment,WeiboSearchLimitedLastComment,WeiboMentionLastComment
import pandas as pd

def main(day_back):
    # Get the last 2000 comments for each post at most
    try:
        weibo = SocialWeiboAPI()
        session = weibo.createSession()


        startTime = weibo.getStrTime(day_back)
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

        df = pd.DataFrame(commentPostList)
        df = df.sort_values(by='since_id',ascending=False)
        df = df.drop_duplicates(subset='id',keep='first')
        commentPostList_dedup = df.to_dict('records')
        weibo.doParallel('comment',commentPostList_dedup)
    finally:
        session.close()

if __name__ =='__main__':
    main(-7)