import pandas as pd
import requests
import json
from .SocialBasicAPI import SocialBasicAPI
import sys


class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__apiToken = self.cfp.get('api','weibo')
	
		
	def getFriendshipsFollowers(self,**kwargs):
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			
			url = 'https://c.api.weibo.com/2/friendships/followers/biz.json'
			
			self.logger.info('Calling getFriendshipsFollowers API')
			"""
			result = self.getRequest(url, paramsDict)
			if result.get('error_code') != None:
				raise KeyError(result)
			"""
			with open('./input/weiboerror.json', 'r') as f:
				result = json.load(f)
			if result.get('erro_code') != None:
				raise KeyError
			newJson = json.dumps(result['users'])
			df = pd.read_json(newJson,orient='records')
			self.logger.info('Total records received:{}'.format(len(df)))
			return df
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)