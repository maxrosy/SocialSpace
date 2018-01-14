import pandas as pd
from .SocialBasicAPI import SocialBasicAPI
import sys, os
import time, datetime
import hashlib
from urllib import parse
from sqlalchemy import bindparam,Table
from zipfile import ZipFile
import math



class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__apiToken = self.cfp.get('api','weibo')

	def getUsersCountBatch(self,uids):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/users/counts_batch/other

		:param uids: seperated by ',', max 100
		:return:
		"""
		self.logger.info("Calling getUsersCountBatch")
		try:
			params_dict = {'access_token':self.__apiToken,'uids':uids}

			url = 'https://c.api.weibo.com/2/users/counts_batch/other.json'

			result = self.getRequest(url, params_dict)
			result = result.json()

			"""
			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			"""
			users = pd.DataFrame(result)

			# Add update_date column
			users['update_date'] = time.strftime("%Y-%m-%d", time.localtime())

			#match column name in table
			users.rename(columns={'id': 'uid'}, inplace=True)
			# Remove previous inserted records for the day
			engine, meta = self.connectToDB('pandas')
			table = Table('weibo_user_growth', meta)
			stmt = table.delete().where(table.c.update_date == time.strftime("%Y-%m-%d", time.localtime()))
			conn = engine.connect()
			res = conn.execute(stmt)

			res.close()
			conn.close()

			self.insertToDB('pandas', 'weibo_user_growth', users, type='dataframe')

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getRepostTimelineAll(self, pid, count=200, **kwargs):
		self.logger.info("Calling getRepostTimelineAll")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			params_dict['id'] = pid
			params_dict['count'] = count

			url = 'https://c.api.weibo.com/2/statuses/repost_timeline/all.json'

			result = self.getRequest(url, params_dict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			total_number = result.get('total_number')
			pages = int(math.ceil(total_number/count))
			repost_list = [self.getRepostTimelineByPage(pid, count, page=page+1)for page in range(pages)]
			df_repost = pd.concat(repost_list, ignore_index=True)
			self.logger.info("Totally {} records in {} pages".format(len(df_repost),pages))

			return df_repost

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getRepostTimelineByPage(self, pid, count=200, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/repost_timeline/all

		:param pid: post id
		:param count: number of return records in one page, max is 200
		:param kwargs:
		:return:
		"""

		self.logger.info("Calling getRepostTimelineByPage")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			params_dict['id'] = pid
			params_dict['count'] = count

			url = 'https://c.api.weibo.com/2/statuses/repost_timeline/all.json'

			result = self.getRequest(url, params_dict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			reposts = result.get('reposts')
			total_number = result.get('total_number')

			df_repost = pd.DataFrame(reposts)

			# Get original post id
			posts = df_repost['retweeted_status']
			post_list = [pd.DataFrame([post]) for post in posts]
			df_post = pd.concat(post_list, ignore_index=True)
			df_repost['original_pid'] = df_post['mid']
			df_repost.drop('retweeted_status',axis=1,inplace=True)

			# Get repost uid
			users = df_repost['user']
			user_list = [pd.DataFrame([user]) for user in users]
			df_user = pd.concat(user_list, ignore_index=True)
			df_repost['uid'] = df_user['id']
			df_repost.drop('user', axis=1, inplace=True)

			# Get retweet source match
			df_repost['source'] = df_repost['source'].apply(self.matchPostSource)

			df_repost_cleaned = df_repost


			self.logger.info("Totally {} records".format(len(df_repost_cleaned)))
			return df_repost_cleaned


		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getUserTimelineBatch(self, uids, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/user_timeline_batch

		:param uids:
		:param kwargs:
		:return:
		"""
		self.logger.info("Calling getUserTimelineBatch")
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

	def getUserTimelineOther(self, trim_user=1,**kwargs):
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
			params_dict['trim_user'] = trim_user
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

	def searchStatusesHistoryCreate(self, starttime, endtime, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/historical/create

		:param starttime: '%Y-%m-%d %H:%M:%S'
		:param endtime: '%Y-%m-%d %H:%M:%S'
		:param kwargs: q, province and ids at least on of the three is required
		:return:
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
			q = result.get('q')
			type = result.get('type')
			hasv = result.get('hasv')
			starttime = result.get('starttime')
			endtime = result.get('endtime')
			ids = result.get('ids')
			provinceId = result.get('province')
			cityId = result.get('city')
			onlynum = result.get('onlynum')

			#Insert new created task into DB
			records = {'task_id': __taskId, 'user_id': __id, 'secret_key': __secretKey, 'starttime': starttime,
						'endtime': endtime, 'type': type, 'hasv': hasv, 'onlynum': onlynum, 'query': q,
						'province': provinceId, 'city': cityId, 'ids': ids}
			self.insertToDB('pandas', 'task_history', records, type='dict')

			self.logger.info("Task {} is created.".format(__taskId))

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
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
			# Get task which is not downloaded yet
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
				paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,
							  'signature':hashlib.md5(pw.encode('utf-8')).hexdigest()}
				result = self.getRequest(url,paramsDict)
				result = result.json()

				if result.get('error_code') is not None:
					raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
				if result.get('status') is True:
					df_post = self.searchStatusesHistoryDownload(taskId,id,secretKey)

					# To do - play with df_post
					self.logger.info("Task {} is done and returns {} records".format(taskId,result.get('count')))
					finishTasks.append(taskId)
			res.close()
			
			if finishTasks:
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
			paramsDict = {'access_token':self.__apiToken,'task_id':taskId,'timestamp':timestamp,
						  'signature':hashlib.md5(pw.encode('utf-8')).hexdigest()}

			result = self.getRequest(url,paramsDict)
			"""
			if result.get('status_code') is not  None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			"""
			downloadUrl = result.url

			r = self.getRequest(downloadUrl)

			# Download zip file
			localzipFile = './output/{}.zip'.format(taskId)
			with open(localzipFile,'wb') as f:
				for chunk in r.iter_content(chunk_size=chunkSize):
					f.write(chunk)
			
			fileInfo = os.stat(localzipFile)
			self.logger.info('{} is downloaded with {} bytes'.format(localzipFile, fileInfo.st_size))

			# Read lof file and create dataframe
			extractFile = '{}.log'.format(taskId)
			with ZipFile(localzipFile) as myzip:
				pwd = str(taskId)+secretKey
				with myzip.open(extractFile, pwd=pwd.encode('utf-8')) as myfile:
					post_lists = myfile.read().splitlines()
					post_lists = [eval(post_list.decode()) for post_list in post_lists]
					df_post = pd.DataFrame(post_lists)

			#print(df_post)
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
			paramsDict['bucket'] = 200
			
			url = 'https://c.api.weibo.com/2/friendships/followers/biz.json'
			
			
			result = self.getRequest(url, paramsDict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			users = result.get('users')
			df = pd.DataFrame(users)
			self.logger.info('Total records received:{}'.format(len(df)))
			previous_cursor = result.get('previous_cursor')
			next_cursor = result.get('next_cursor')
			if next_cursor != 0:
				self.getFriendshipsFollowers(max_time=next_cursor)

			return

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def getStatusesShowBatch(self, ids, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/show_batch/biz

		:param ids: post ids seperated by ','. 50 maximum
		:param kwargs:
		:return:
		"""
		
		self.logger.info("Calling getStatusesShowBatch")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['ids'] = ids
			
			url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'

			result = self.getRequest(url, paramsDict)
			result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			posts = result.get('statuses')
			df_post = pd.DataFrame(posts)

			if paramsDict.get('trim_user',0) != 1: # 1 means return uid only
				users = df_post['user']
				user_list = [pd.DataFrame([user]) for user in users]
				df_user = pd.concat(user_list, ignore_index=True)
				df_post['uid'] = df_user['id']
				df_post.drop('user', axis=1, inplace=True)

			df_post_cleaned = df_post  # self.cleanRecords(df_post)

			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def getSearchStatusesLimited(self,q,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/search/statuses/limited

		:param q:
		:param kwargs:
		:return:
		"""
		self.logger.info("Calling getSearchStatusesLimited function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['q'] = parse.quote(q)
			
			url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'

			result = self.getRequest(url, paramsDict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			posts = result.get('statuses')

			df_post = pd.DataFrame(posts)

			users = df_post['user']
			userList = [pd.DataFrame([user]) for user in users ]
			df_user = pd.concat(userList,ignore_index=True)
			df_post['user_id'] = df_user['id']
			df_post.fillna('null',inplace=True)
			df_post.drop('user',axis=1,inplace=True)

			df_post_cleaned = df_post
			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def getCommentsShow(self,mid,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/comments/show/all

		:param id:
		:param kwargs:
		:return:
		"""
		self.logger.info("Calling getCommentsShow function")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['id'] = mid

			url = 'https://c.api.weibo.com/2/comments/show/all.json'

			result = self.getRequest(url, paramsDict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			comments = result.get('comments')
			df_comments = pd.DataFrame(comments)
			
			# Extract comment user_id
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

			# get comment source match
			df_comments['source'] = df_comments['source'].apply(self.matchPostSource)

			return df_comments

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
