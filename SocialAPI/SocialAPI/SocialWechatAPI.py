import pandas as pd
import requests
import json
from .SocialBasicAPI import SocialBasicAPI
import sys

class SocialWechatAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWechatAPI,self).__init__()
		
	def getUserSummary(self, begin_date, end_date):
		try:
			url = 'https://api.weixin.qq.com/datacube/getusersummary?access_token=%s' %self.apitoken
			data = {'begin_date': begin_date,'end_date' : end_date}
			postData = json.dumps(data)
		
			self.logger.info('Calling getUserSummary API')
			
			#result = self.postRequest(url,postData)
			
			with open('./input/wechattest.json', 'r') as f:
				data = json.load(f)
			newJson = json.dumps(data['list'])
			df = pd.read_json(newJson,orient='records')
			self.logger.info('Total records received:%d' %(len(df)))
			return df
			
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
		
	def getUserCumulate(self, begin_date, end_date):
		url = 'https://api.weixin.qq.com/datacube/getusercumulate?access_token=%s' %self.apitoken
		data = {'begin_date': begin_date,'end_date' : end_date}
		postData = json.dumps(data)
		try:
			self.logger.info('Calling getUserCimulate API')
			#r = requests.post(url, data=postData)
			return
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def __str__(self):
		return 'Social API of Wechat'