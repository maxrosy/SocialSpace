from SocialAPI.SocialAPI.SocialWechatAPI import SocialWechatAPI
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
import os

if __name__ == '__main__':
	os.chdir(os.path.dirname(os.path.realpath(__file__)))
	wechat=SocialWechatAPI()
	b=wechat.getUserSummary('a','b')
	#c=a.cleanRecords(b)
	wechat.writeDataFrameToCsv(b,'./output/panda_output.xlsx','Wechat')
	wechat.syncToDB(b,'pandas','wechat')
	
	weibo=SocialWeiboAPI()
	c=weibo.getFriendshipsFollowers()
	weibo.writeDataFrameToCsv(c,'./output/panda_output.xlsx','Weibo')
	weibo.syncToDB(c,'pandas','weibo')
	
	
	