import pandas as pd
import requests
import json
from .SocialBasicAPI import SocialBasicAPI
import sys
import configparser

class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__cfp = configparser.ConfigParser()
		self.__cfp.read('./conf/social.conf')
		self.__apiToken = self.__cfp.get('api','weibo')
	
		
	def getFriendshipsFollowers(self,**kwargs):
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			
			url = 'https://c.api.weibo.com/2/friendships/followers/biz.json'
			#result = self.getRequest(url, paramsDict)
			
			self.logger.info('Calling getFriendshipsFollowers API')
			
			with open('./input/weibotest.json', 'r') as f:
				data = json.load(f)
			newJson = json.dumps(data['users'])
			df = pd.read_json(newJson,orient='records')
			self.logger.info('Total records received:%d' %(len(df)))
			return df
			
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)