import pandas as pd
import requests
import json
from .SocialBasicAPI import SocialBasicAPI
import sys
import time
import hashlib
from sqlalchemy import bindparam,Table



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
			
			
			#result = self.postRequst(url,paramsDict)
			
			with open('./input/weibo_history_create.json', 'r') as f:
				result = json.load(f)
			if result.get('erro_code') != None:
				raise KeyError
			__id = result.get('id')
			__taskId = int(result.get('task_id'))
			__secretKey = result.get('secret_key')
			
			# Insert new created task into DB
			engine, meta = self.connectToDB('pandas')
			Task = Table('task_history',meta)
			ins = Task.insert().values(task_id=__taskId, user_id=__id, secret_key=__secretKey)
			conn = engine.connect()
			result = conn.execute(ins)
			
			result.close()
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
			finishTasks = []
			# Get task whose status is 0
			engine, meta = self.connectToDB('pandas')
			Task = Table('task_history',meta)
			stmt = Task.select().where(Task.c.status==0)
			conn = engine.connect()
			res = conn.execute(stmt)
			
			for record in res.fetchall():
				timestamp = int(time.time())
				taskId = record.task_id
				id = record.user_id
				secretKey = record.secret_key
				pw = id+secretKey+str(timestamp)
				paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(pw.encode('utf-8'))}
				#result = self.getRequest(url,paramsDict)
				with open('./input/weibo_history_check.json', 'r') as f:
					result = json.load(f)
				if result.get('erro_code') != None:
					raise KeyError
				if result.get('status') == True:
					#self.searchStatusesHistoryDownload(taskId,id,secretKey)
					self.logger.info("Task {} is done and returns {} records".format(taskId,result.get('count')))
					finishTasks.append(taskId)
			res.close()
			
			if finishTasks != []:
				try:
					# Update in executemany mode using bindparam
					paramList = [{'task':task} for task in finishTasks]	
					stmt = Task.update().values(status=1).where(Task.c.task_id==bindparam('task'))
					res = conn.execute(stmt,paramList)
				
				except Exception as e:
					self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
				finally:
					res.close()
					conn.close()
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def searchStatusesHistoryDownload(self,taskId,id,secretKey):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/download
		"""
		self.logger.info("Calling searchStatusesHistoryDownload function")
		try:
			url = 'https://c.api.weibo.com/2/search/statuses/historical/download.json'
			timestamp = int(time.time())
			pw = id+secretKey+str(timestamp)
			paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(pw.encode('utf-8'))}	
			
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
			
			
			#result = self.getRequest(url, paramsDict)
			
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
	
	def getStatusesShowBatch(self, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/show_batch/biz
		"""
		
		self.logger.info("Calling getStatusesShowBatch")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			
			url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'
			#result = self.getRequest(url, paramsDict)
			
			with open('./input/weibo_status_show_batch.json', 'r') as f:
				result = json.load(f)
			if result.get('erro_code') != None:
				raise KeyError
			posts = result.get('statuses')
			df_post = pd.read_json(json.dumps(posts),orient='records')
			users = df_post['user']
			userList = [pd.DataFrame([user]) for user in users ]
			df_user = pd.concat(userList,ignore_index=True)
			df_post['user_id'] = df_user['id']
			df_post['annotations'] = ''
			df_post.fillna('null',inplace=True)
			df_post.drop('user',axis=1,inplace=True)
			try:
				engine, meta = self.connectToDB('pandas')
				conn = engine.connect()
				
				User = Table('weibo_user',meta)
				stmt = User.insert()	
				res = conn.execute(stmt,df_user.to_dict('records'))
				self.logger.info('{} User record(s) have been inserted'.format(res.rowcount))
				res.close()
				
				Post = Table('weibo_post',meta)
				stmt = Post.insert()
				res = conn.execute(stmt,df_post.to_dict('records'))
				self.logger.info('{} Post record(s) have been inserted'.format(res.rowcount))
				res.close()
			except Exception as e:
				self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			finally:
				
				conn.close()
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)