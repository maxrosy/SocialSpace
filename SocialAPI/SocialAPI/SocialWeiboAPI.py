import pandas as pd
import json
from .SocialBasicAPI import SocialBasicAPI
import sys, os
import time, datetime
import hashlib
from urllib import parse
from sqlalchemy import bindparam,Table
from zipfile import ZipFile



class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__apiToken = self.cfp.get('api','weibo')

	def getStatusesUserTimelineBatch(self, uids, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/user_timeline_batch

		:param kwargs:
		:return:
		"""
		self.logger.info("Calling getStatusesUserTimelineBatch")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			params_dict['uids'] = uids

			url = 'https://c.api.weibo.com/2/statuses/user_timeline_batch.json'

			result = self.getRequest(url, params_dict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			statuses = result.get('statuses')
			df_post = pd.DataFrame(statuses)
			users = df_post['user']
			user_list = [pd.DataFrame([user]) for user in users]
			df_user = pd.concat(user_list, ignore_index=True)
			df_post['user_id'] = df_user['id']
			df_post['annotations'] = ''
			df_post.drop('user', axis=1, inplace=True)
			df_post_cleaned = df_post

			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getStatusesUserTimelineOther(self, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/user_timeline/other

		:param kwargs:
		:return:
		"""
		self.logger.info("Calling getStatusesUserTimelineOther")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			#start_time = params_dict.get('start_time',self.getStrTime(1))
			#params_dict['start_time'] = self.getTimeStamp(start_time)

			url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'

			result = self.getRequest(url, params_dict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'),result.get('error')))

			statuses = result.get('statuses')
			df_post = pd.DataFrame(statuses)

			if params_dict.get('trim_user',0) != 1: # 1 means return uid only
				users = df_post['user']
				user_list = [pd.DataFrame([user]) for user in users]
				df_user = pd.concat(user_list, ignore_index=True)
				df_post['uid'] = df_user['id']
				df_post.drop('user', axis=1, inplace=True)

			df_post_cleaned = df_post #self.cleanRecords(df_post)

			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)

	def searchStatusesHistoryCreate(self,starttime,endtime, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/create

		In kwargs, q, province and ids at least on of the three is required
		"""
		self.logger.info("Calling searchStatusesHistoryCreate function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['starttime'] = self.getTimeStamp(starttime)
			paramsDict['endtime'] = self.getTimeStamp(endtime)
			
			url = 'https://c.api.weibo.com/2/search/statuses/historical/create.json'
			
			
			result = self.postRequest(url,paramsDict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			__id = result.get('id')
			__taskId = int(result.get('task_id'))
			__secretKey = result.get('secret_key')
			
			# Insert new created task into DB
			try:
				records = {'task_id':__taskId,'user_id':__id,'secret_key':__secretKey}
				self.insertToDB('pandas','task_history',records,type='dict')
				
				self.logger.info("Task {} is created.".format(__taskId))
			except Exception as e:
				self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
				exit(1)
			
			return

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
				timestamp = int(time.time()*1000)
				taskId = record.task_id
				id = record.user_id
				secretKey = record.secret_key
				pw = id+secretKey+str(timestamp)
				paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(pw.encode('utf-8')).hexdigest()}
				result = self.getRequest(url,paramsDict)
				result = result.json()

				if result.get('error_code') is not None:
					raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
				if result.get('status') is True:
					self.searchStatusesHistoryDownload(taskId,id,secretKey)
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
			
			return finishTasks

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def searchStatusesHistoryDownload(self,taskId,id,secretKey,chunkSize=1024):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/download
		"""
		self.logger.info("Calling searchStatusesHistoryDownload function")
		try:
			url = 'https://c.api.weibo.com/2/search/statuses/historical/download.json'
			timestamp = int(time.time()*1000)
			pw = id+secretKey+str(timestamp)
			paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,'signature':hashlib.md5(pw.encode('utf-8')).hexdigest()}

			result = self.getRequest(url,paramsDict)
			"""
			if result.get('status_code') is not  None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			"""
			downloadUrl = result.url

			r = self.getRequest(downloadUrl)

			localzipFile = './output/{}.zip'.format(taskId)
			with open(localzipFile,'wb') as f:
				for chunk in r.iter_content(chunk_size=chunkSize):
					f.write(chunk)
			
			fileInfo = os.stat(localzipFile)
			self.logger.info('{} is downloaded with {} bytes'.format(localzipFile, fileInfo.st_size))

			extractFile = '{}.log'.format(taskId)
			with ZipFile(localzipFile) as myzip:
				pwd = str(taskId)+secretKey
				with myzip.open(extractFile, pwd=pwd.encode('utf-8')) as myfile:
					post_lists = myfile.read().splitlines()
					post_lists = [eval(post_list.decode()) for post_list in post_lists]
					df_post = pd.DataFrame(post_lists)
			print(df_post)
			return df_post

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def getFriendshipsFollowers(self,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/friendships/followers/biz
		"""
		self.logger.info("Calling getFriendshipsFollowers function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			
			url = 'https://c.api.weibo.com/2/friendships/followers/biz.json'
			
			
			#result = self.getRequest(url, paramsDict)
			
			with open('./input/weibotest.json', 'r') as f:
				result = json.load(f)
			if result.get('error_code') != None:
				raise KeyError
			newJson = json.dumps(result['users'])
			df = pd.read_json(newJson,orient='records',dtype=object)
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
			if result.get('error_code') != None:
				raise KeyError
			posts = result.get('statuses')
			df_post = pd.read_json(json.dumps(posts),orient='records',dtype=object)
			users = df_post['user']
			userList = [pd.DataFrame([user]) for user in users ]
			df_user = pd.concat(userList,ignore_index=True)
			df_post['user_id'] = df_user['id']
			df_post['annotations'] = ''
			df_post.fillna('null',inplace=True)
			df_post.drop('user',axis=1,inplace=True)
			
			return (df_user,df_post)
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def getSearchStatusesLimited(self,q,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/limited
		"""
		self.logger.info("Calling getSearchStatusesLimited function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramList['q'] = parse.quote(q)
			
			url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'
			#result = self.getRequest(url, paramsDict)
			
			with open('./input/weibo_status_show_batch.json', 'r') as f:
				result = json.load(f)
			if result.get('error_code') != None:
				raise KeyError
			posts = result.get('statuses')
			df_post = pd.read_json(json.dumps(posts),orient='records',dtype=object)
			users = df_post['user']
			userList = [pd.DataFrame([user]) for user in users ]
			df_user = pd.concat(userList,ignore_index=True)
			df_post['user_id'] = df_user['id']
			df_post['annotations'] = ''
			df_post.fillna('null',inplace=True)
			df_post.drop('user',axis=1,inplace=True)
			return df_post
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def getCommentsShow(self,id,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/comments/show/all
		"""
		self.logger.info("Calling getCommentsShow function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['id'] = id
			url = 'https://c.api.weibo.com/2/comments/show/all.json'
			#result = self.getRequest(url, paramsDict)
			
			with open('./input/weibo_comments_show.json', 'r') as f:
				result = json.load(f)
			if result.get('error_code') != None:
				raise KeyError
			comments = result.get('comments')
			df_comments = pd.read_json(json.dumps(comments),orient='records',dtype=object)
			
			# Extract user_id 
			comment_users = df_comments['user']
			comment_users_list = [pd.DataFrame([comment_user]) for comment_user in comment_users]
			df_comment_user = pd.concat(comment_users_list,ignore_index=True)
			df_comments['user_id'] = df_comment_user['id']
			df_comments.drop('user',axis=1,inplace=True)
			
			# Extract post_id
			comment_posts = df_comments['status']
			comment_posts_list = [pd.DataFrame([comment_post]) for comment_post in comment_posts]
			df_comment_post = pd.concat(comment_posts_list,ignore_index=True)
			df_comments['post_id'] = df_comment_post['id']
			df_comments.drop('status',axis=1,inplace=True)
			
			return df_comments
			
		except KeyError:
			self.logger.error('On line {} - Error Code: {}, Error Msg: {}'.format(sys.exc_info()[2].tb_lineno,result['error_code'],result['error']))
			exit(1)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
