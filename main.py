from SocialAPI.SocialAPI.SocialWechatAPI import SocialWechatAPI
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
import os

if __name__ == '__main__':
	os.chdir(os.path.dirname(os.path.realpath(__file__)))
	
	#wechat=SocialWechatAPI()
	#b=wechat.getUserSummary('a','b')
	#c=a.cleanRecords(b)
	#wechat.writeDataFrameToExcel(b,'./output/wechat.xlsx','Wechat')
	#wechat.writeDataFrameToCsv(b,'./output/wechat.csv','|')
	#wechat.syncToDB(b,'pandas','wechat')
	"""
	weibo=SocialWeiboAPI()
	c=weibo.getFriendshipsFollowers()
	weibo.writeDataFrameToExcel(c,'./output/weibo.xlsx','Weibo')
	weibo.writeDataFrameToCsv(c,'./output/weibo.csv','|')
	weibo.syncToDB(c,'pandas','weibo')
	weibo.writeToS3('friso-test','./output/weibo.csv','share/weibo.csv')
	"""
	
	weibo=SocialWeiboAPI()
	weibo.searchStatusesHistoryCreate('a','b')
	weibo.searchStatusesHistoryCheck('wx655b00eace11403d','aa98e239adde8e58c2cb1d50b250efb2')
	#users, posts = weibo.getStatusesShowBatch()
	#weibo.writeDataFrameToCsv(users,'./output/weibo_users.csv','|')
	#weibo.writeDataFrameToCsv(posts,'./output/weibo_posts.csv','|')
	#weibo.insertToDB('pandas','weibo_user',users)
	#weibo.insertToDB('pandas','weibo_post',posts)
	#comments = weibo.getCommentsShow(3)
	#weibo.writeDataFrameToCsv(comments,'./output/weibo_comments.csv','|')
	#weibo.insertToDB('pandas','weibo_comment',comments)
	
	
	
