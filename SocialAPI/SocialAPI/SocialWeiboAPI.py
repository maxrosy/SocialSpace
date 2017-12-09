import pandas as pd
import requests
import json
from .SocialBasicAPI import SocialBasicAPI
import sys
import time
import hashlib



class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__apiToken = self.cfp.get('api','weibo')
	
	def searchStatusesHistoryCreate(self,starttime,endtime, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/create
		"""
		self.logger.info("Calling searchStatusesHistoryCreate function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['starttime'] = starttime
			paramsDict['endtime'] = endtime
			
			url = 'https://c.api.weibo.com/2/search/statuses/historical/create.json'
			
			"""
			result = self.postRequst(url,paramsDict)
			if result.get('error_code') != None:
				raise KeyError(result)
			"""
			with open('./input/weibo_history_create.json', 'r') as f:
				result = json.load(f)
			if result.get('erro_code') != None:
				raise KeyError
			__id = result.get('id')
			__taskId = int(result.get('task_id'))
			__secretKey = result.get('secret_key')
			
			conn = self.connectToDB('pandas')
			cursor=conn.cursor()
			cursor.execute("insert into task_history(task_id,user_id,secret_key) values({},{},{})".format(__id,__taskId,__secretKey))
			conn.commit()
			cursor.close()
			conn.close()
			self.logger.info("Task {} is created.".format(__taskId))
			return
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def searchStatusesHistoryCheck(self):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/check
		"""
		self.logger.info("Calling searchStatusesHistoryCheck function")
		try:
			url = 'https://c.api.weibo.com/2/search/statuses/historical/check.json'
			
			conn = self.connectToDB('pandas')
			cursor=conn.cursor()
			cursor.execute("select task_id,user_id,secret_key from task_history where status =0")
			for record in cursor.fetchall():
				timestamp = int(time.time())
				taskId = record[0]
				id = record[1]
				secretKey = record[2]
				pw = id+secretKey+str(timestamp)
				paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(pw.encode('utf-8'))}
				#result = self.getRequest(url,paramsDict)
				with open('./input/weibo_history_check.json', 'r') as f:
					result = json.load(f)
				if result.get('erro_code') != None:
					raise KeyError
				if result.get('status') == True:
					self.logger.info("Task {} is done and returns {} records".format(taskId,result.get('count')))
					cursor.execute("update task_history set status=1 where task_id={}".format(taskId))
					conn.commit()
			
			cursor.close()
			conn.close()
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def searchStatusesHistoryDownload(self,id,taskId,secretKey):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/download
		"""
		self.logger.info("Calling searchStatusesHistoryDownload function")
		try:
			url = 'https://c.api.weibo.com/2/search/statuses/historical/download.json'
			timestamp = int(time.time())
			paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(id+secretKey+timestamp)}	
			
			result = self.getRequest(url,paramsDict)
			if result.get('erro_code') != None:
				raise KeyError
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	def getFriendshipsFollowers(self,**kwargs):
		self.logger.info("Calling getFriendshipsFollowers function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			
			url = 'https://c.api.weibo.com/2/friendships/followers/biz.json'
			
			"""
			result = self.getRequest(url, paramsDict)
			if result.get('error_code') != None:
				raise KeyError(result)
			"""
			with open('./input/weibotest.json', 'r') as f:
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