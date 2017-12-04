from SocialAPI.SocialAPI.SocialWechatAPI import SocialWechatAPI
import os

if __name__ == '__main__':
	os.chdir(os.path.dirname(os.path.realpath(__file__)))
	a=SocialWechatAPI()
	b=a.getUserSummary('a','b')
	#c=a.cleanRecords(b)
	a.writeDataFrameToCsv(b,'./output/panda_output.xlsx','newSheet1')
	a.syncToDB(b,'pandas','wechat')
	
	