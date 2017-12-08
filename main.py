from SocialAPI.SocialAPI.SocialWechatAPI import SocialWechatAPI
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
import os

if __name__ == '__main__':
	os.chdir(os.path.dirname(os.path.realpath(__file__)))
	wechat=SocialWechatAPI()
	b=wechat.getUserSummary('a','b')
	#c=a.cleanRecords(b)
	wechat.writeDataFrameToExcel(b,'./output/wechat.xlsx','Wechat')
	wechat.writeDataFrameToCsv(b,'./output/wechat.csv','|')
	wechat.syncToDB(b,'pandas','wechat')
	
	weibo=SocialWeiboAPI()
	c=weibo.getFriendshipsFollowers()
	weibo.writeDataFrameToExcel(c,'./output/panda_output.xlsx','Weibo')
	weibo.writeDataFrameToCsv(c,'./output/weibo.csv','|')
	weibo.syncToDB(c,'pandas','weibo')
	weibo.writeToS3('friso-test','./output/weibo.csv','share/weibo.csv')
	
	
	
	