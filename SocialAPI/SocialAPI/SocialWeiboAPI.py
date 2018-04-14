import pandas as pd
from .SocialBasicAPI import SocialBasicAPI
import sys, os
import time, datetime
import hashlib
from urllib import parse
from zipfile import ZipFile
from functools import reduce
from ..Model import User,UserGrowth,UserTag,Comment,PostStatus,TaskHistory,Media
from sqlalchemy import func
from SocialAPI.Helper import Helper


class SocialWeiboAPI(SocialBasicAPI):

	def __init__(self):
		super(SocialWeiboAPI,self).__init__()
		self.__apiToken = self.cfp.get('api','weibo')
		self.__rootPath = Helper().getRootPath()

	async def getUserShowBatchOther(self,uids):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/users/show_batch/other

		:param uids: seperated by ',', max 50
		:return:
		"""
		self.logger.info("Calling getUserShowBatchOther")
		try:
			params_dict = {'access_token': self.__apiToken, 'uids': uids}
			url = 'https://c.api.weibo.com/2/users/show_batch/other.json'

			result = await self.getAsyncRequest(url, params_dict)

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			users = result.get('users')
			if not users:
				raise Exception("No data returned")
			df_user = pd.DataFrame(users)

			df_user_cleaned = self.cleanRecords(df_user, renameColumns={'id': 'uid'},dropColumns=['status'])
			self.upsertToDB(User,df_user_cleaned)
			return df_user_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	async def getUsersCountBatch(self,uids):
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

			result = await self.getAsyncRequest(url,params_dict)

			"""
			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
			"""
			users = pd.DataFrame(result)

			# Add update_date column
			users['update_date'] = time.strftime("%Y-%m-%d", time.localtime())
			users['year'] = datetime.datetime.now().year
			users['month'] = datetime.datetime.now().month
			users['day'] = datetime.datetime.now().day

			df_user_cleaned = self.cleanRecords(users,renameColumns={'id': 'uid'},utcTimeCovert=False)
			self.upsertToDB(UserGrowth,df_user_cleaned)
			return df_user_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getRepostTimelineAll(self, pid, **kwargs):
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
			params_dict['count'] = params_dict.get('count',200)

			url = 'https://c.api.weibo.com/2/statuses/repost_timeline/all.json'
			page = 0
			df_list = []
			loop = True

			while loop:
				try:
					page += 1
					params_dict['page'] = page
					result = self.getRequest(url, params_dict)
					result = result.json()

					if result.get('error_code') is not None:
						raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

					reposts = result.get('reposts')

					if not reposts:
						raise StopIteration

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

					df_list.append(df_repost)

					self.logger.debug("Totally {} records in page {}".format(len(df_repost), page))

				except StopIteration as e:
					self.logger.debug("Totally {} page(s)".format(page-1))
					loop = False

			df_repost_cleaned = pd.concat(df_list, ignore_index=True)
			self.logger.info("Totally {} records in {} page(s)".format(len(df_repost_cleaned), page-1))
			return df_repost_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getUserTimelineBatch(self, uids, **kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/user_timeline_batch

		:param uids: seperated by ',', max 20
		:param kwargs:
		:return: return at maxium 200 records for each uid
		"""
		def f(x):
			"""
			:param x:[id,{},{}...]
			:return: [{'pid':id,},{'pid':id,}...]
			"""
			pid = x.pop(0)
			for item in x:
				item['pid'] = pid
			return x

		self.logger.info("Calling getUserTimelineBatch")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			params_dict['uids'] = uids
			#params_dict['count'] = params_dict.get('count',200)

			url = 'https://c.api.weibo.com/2/statuses/user_timeline_batch.json'

			page = 0
			df_list = []
			dropColumns = []
			loop = True

			while loop:
				try:
					page += 1
					params_dict['page'] = page
					result = self.getRequest(url, params_dict)
					result = result.json()

					if result.get('error_code') is not None:
						raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

					statuses = result.get('statuses')
					if not statuses:
						raise StopIteration
					df_post = pd.DataFrame(statuses)
					users = df_post['user']
					user_list = [pd.DataFrame([user]) for user in users]
					df_user = pd.concat(user_list, ignore_index=True)
					df_post['uid'] = df_user['id']
					df_post['source'] = df_post['source'].apply(self.matchPostSource)

					df_list.append(df_post)
					self.logger.info("Totally {} records in page {}".format(len(df_post), page))
				except StopIteration:
					self.logger.info("Totally {} page(s)".format(page-1))
					loop = False

			df_post = pd.concat(df_list, ignore_index=True)
			df_post['has_url_objects'] = False
			df_post['is_retweeted'] = False
			dropColumns.append('user')
			if 'retweeted_status' in df_post.columns:
				dropColumns.append('retweeted_status')
				df_post['is_retweeted'] = ~df_post['retweeted_status'].isnull()
				df_post['retweeted_status'].where(df_post['retweeted_status'].notnull(), None, inplace=True)
				df_post['retweeted_id'] = df_post['retweeted_status'].apply(lambda x: x['id'] if x else None)
			if 'url_objects' in df_post.columns:
				dropColumns.append('url_objects')
				df_post['has_url_objects'] = df_post['url_objects'].apply(lambda x: True if x else False)
				df_post['url_objects'] = df_post['id'].apply(lambda x: [x])+df_post['url_objects']
				df_post['url_objects'] = df_post['url_objects'].apply(f)
				df_url_objects_list = reduce(lambda x,y: x+y, df_post['url_objects'])
				df_url_objects = pd.DataFrame(df_url_objects_list)
				df_url_objects_cleaned = self.cleanRecords(df_url_objects, utcTimeCovert=False)
				self.upsertToDB(Media, df_url_objects_cleaned)

			df_post_cleaned = self.cleanRecords(df_post,dropColumns=dropColumns)
			self.logger.info("Totally {} records in {} page(s)".format(len(df_post_cleaned), page-1))
			self.upsertToDB(PostStatus,df_post_cleaned)


			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

	def getUserTimelineOther(self, uid,**kwargs):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/statuses/user_timeline/other

		:param kwargs:
		:return:
		"""
		def f(x):
			"""
			:param x:[id,{},{}...]
			:return: [{'pid':id,},{'pid':id,}...]
			"""
			pid = x.pop(0)
			for item in x:
				item['pid'] = pid
			return x

		self.logger.info("Calling getStatusesUserTimelineOther")
		try:
			params_dict = kwargs
			params_dict['access_token'] = self.__apiToken
			params_dict['uid'] = uid
			params_dict['trim_user'] = params_dict.get('trim_user',1)
			#params_dict['start_time'] = self.getTimeStamp('2018-01-14 00:00:00')
			#params_dict['end_time'] = self.getTimeStamp('2018-01-15 00:00:00')

			url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'
			page = 0
			df_list = []
			dropColumns = []
			loop = True

			while loop:
				try:
					page += 1
					params_dict['page'] = page
					result = self.getRequest(url, params_dict)
					result = result.json()

					if result.get('error_code') is not None:
						raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'),result.get('error')))

					statuses = result.get('statuses')
					if not statuses:
						raise StopIteration

					df_post = pd.DataFrame(statuses)
					"""
					if params_dict.get('trim_user',0) != 1: # 1 means return uid only
						users = df_post['user']
						user_list = [pd.DataFrame([user]) for user in users]
						df_user = pd.concat(user_list, ignore_index=True)
						df_post['uid'] = df_user['id']
					"""
					df_post['source'] = df_post['source'].apply(self.matchPostSource)
					df_list.append(df_post)
					self.logger.debug("Totally {} records in page {}".format(len(df_post), page))
				except StopIteration:
					self.logger.debug("Totally {} page(s)".format(page-1))
					loop = False
			df_post = pd.concat(df_list, ignore_index=True)

			df_post['has_url_objects'] = False
			df_post['is_retweeted'] = False

			if 'retweeted_status' in df_post.columns:
				dropColumns.append('retweeted_status')
				df_post['is_retweeted'] = ~df_post['retweeted_status'].isnull()
				df_post['retweeted_status'].where(df_post['retweeted_status'].notnull(), None, inplace=True)
				df_post['retweeted_id'] = df_post['retweeted_status'].apply(lambda x: x['id'] if x else None)
			if 'url_objects' in df_post.columns:
				dropColumns.append('url_objects')
				df_post['has_url_objects'] = df_post['url_objects'].apply(lambda x: True if x else False)
				df_post['url_objects'] = df_post['id'].apply(lambda x: [x])+df_post['url_objects']
				df_post['url_objects'] = df_post['url_objects'].apply(f)
				df_url_objects_list = reduce(lambda x,y: x+y, df_post['url_objects'])
				df_url_objects = pd.DataFrame(df_url_objects_list)

			df_post_cleaned = self.cleanRecords(df_post,dropColumns=dropColumns)
			df_url_objects_cleaned = self.cleanRecords(df_url_objects,utcTimeCovert=False)
			self.logger.info("Totally {} records in {} page(s)".format(len(df_post_cleaned), page - 1))
			self.upsertToDB(PostStatus, df_post_cleaned)
			self.upsertToDB(Media,df_url_objects_cleaned)

			return df_post_cleaned, df_url_objects_cleaned

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
			paramsDict['starttime'] = self.getTimeStamp(starttime,'ms')
			paramsDict['endtime'] = self.getTimeStamp(endtime,'ms')
			
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
			self.insertToDB(TaskHistory, records)

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
			"""
			# Old solution using exression SQL
			engine, meta = self.connectToDB('pandas')
			Task = Table('task_history',meta)
			stmt = Task.select().where(Task.c.status==0)
			conn = engine.connect()
			res = conn.execute(stmt)

			"""
			#New solution using ORM
			session = self.createSession()
			for record in session.query(TaskHistory).filter(TaskHistory.status == 0).all():

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

					finishTasks.append({'task_id':taskId,'status':1})
			
			if finishTasks:
				try:

					session.bulk_update_mappings(TaskHistory,finishTasks)
					session.commit()
					"""
					# Update in executemany mode using bindparam
					paramList = [{'task':task} for task in finishTasks]	
					stmt = Task.update().values(status=1).where(Task.c.task_id==bindparam('task'))
					res = conn.execute(stmt,paramList)
					"""
				except Exception as e:
					self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
					session.rollback()
					session.close()
					exit(1)
				finally:
					session.close()
			
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
			localzipFile = self.__rootPath + '/output/{}.zip'.format(taskId)
			with open(localzipFile,'wb') as f:
				for chunk in r.iter_content(chunk_size=chunkSize):
					f.write(chunk)
			
			fileInfo = os.stat(localzipFile)
			self.logger.info('{} is downloaded with {} bytes'.format(localzipFile, fileInfo.st_size))

			# Read log file and create dataframe
			extractFile = self.__rootPath + '{}.log'.format(taskId)
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
		def f(x):
			"""
			:param x:[id,{},{}...]
			:return: [{'pid':id,},{'pid':id,}...]
			"""
			pid = x.pop(0)
			for item in x:
				item['pid'] = pid
			return x

		dropColumns =[]
		self.logger.info("Calling getStatusesShowBatch")
		try:
			paramsDict = kwargs
			paramsDict['access_token'] = self.__apiToken
			paramsDict['ids'] = ids
			
			url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'

			result = self.getRequest(url, paramsDict)
			result = result.json()

			if result.get('error_code') is not None:
				raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

			posts = result.get('statuses')
			df_post = pd.DataFrame(posts)

			if paramsDict.get('trim_user',0) != 1: # 1 means return uid only
				users = df_post['user']
				user_list = [pd.DataFrame([user]) for user in users]
				df_user = pd.concat(user_list, ignore_index=True)
				df_post['uid'] = df_user['id']
				dropColumns.append('user')

			df_post['has_url_objects'] = False
			df_post['is_retweeted'] = False

			if 'retweeted_status' in df_post.columns:
				dropColumns.append('retweeted_status')
				df_post['is_retweeted'] = ~df_post['retweeted_status'].isnull()
				df_post['retweeted_status'].where(df_post['retweeted_status'].notnull(), None, inplace=True)
				df_post['retweeted_id'] = df_post['retweeted_status'].apply(lambda x: x['id'] if x else None)

			if 'url_objects' in df_post.columns:
				dropColumns.append('url_objects')
				df_post['has_url_objects'] = df_post['url_objects'].apply(lambda x: True if x else False)
				df_post['url_objects'] = df_post['id'].apply(lambda x: [x])+df_post['url_objects']
				df_post['url_objects'] = df_post['url_objects'].apply(f)
				df_url_objects_list = reduce(lambda x,y: x+y, df_post['url_objects'])
				df_url_objects = pd.DataFrame(df_url_objects_list)
				#df_url_objects['uuid'] = [uuid.uuid1() for i in range(len(df_url_objects))]

			df_post_cleaned = self.cleanRecords(df_post,dropColumns=dropColumns)
			df_url_objects_cleaned = self.cleanRecords(df_url_objects, utcTimeCovert=False)
			self.upsertToDB(PostStatus, df_post_cleaned)
			self.upsertToDB(Media, df_url_objects_cleaned)
			return df_post_cleaned, df_url_objects_cleaned

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

			page = 0
			df_list = []
			loop = True

			while loop:
				try:
					page += 1
					paramsDict['page'] = page
					result = self.getRequest(url, paramsDict)
					result = result.json()

					if result.get('error_code') is not None:
						raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

					posts = result.get('statuses')
					if not posts:
						raise StopIteration
					df_post = pd.DataFrame(posts)

					users = df_post['user']
					userList = [pd.DataFrame([user]) for user in users ]
					df_user = pd.concat(userList,ignore_index=True)
					df_post['user_id'] = df_user['id']


					df_list.append(df_post)
					self.logger.debug("Totally {} records in page {}".format(len(df_post), page))
				except StopIteration:
					self.logger.debug("Totally {} page(s)".format(page - 1))
					loop = False
			df_post = pd.concat(df_list, ignore_index=True)
			df_post_cleaned = self.cleanRecords(df_post,dropColumns=['user'],utcTimeCovert=True)
			self.logger.info("Totally {} records in {} page(s)".format(len(df_post_cleaned), page-1))
			return df_post_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)


	def getCommentsShow(self,mid,latest=True,**kwargs):
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
			if latest:
				session = self.createSession()
				since_id = session.query(func.max(Comment.id)).filter_by(pid = mid).scalar()
				if since_id:
					paramsDict['since_id'] = since_id
				session.close()
			url = 'https://c.api.weibo.com/2/comments/show/all.json'
			page = 0
			df_list = []
			loop = True

			while loop:
				try:
					page += 1
					paramsDict['page'] = page
					result = self.getRequest(url, paramsDict)
					result = result.json()

					if result.get('error_code') is not None:
						raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

					comments = result.get('comments')
					if not comments or page == 11: # Since there are too many comments, stop after 10 pages to call DB upsert
						raise StopIteration

					df_comment = pd.DataFrame(comments)

					# Extract comment user_id
					comment_users = df_comment['user']
					comment_users_list = [pd.DataFrame([comment_user]) for comment_user in comment_users]
					df_comment_user = pd.concat(comment_users_list,ignore_index=True)
					df_comment['uid'] = df_comment_user['id']

					# Extract post_id
					comment_posts = df_comment['status']
					comment_posts_list = [pd.DataFrame([comment_post]) for comment_post in comment_posts]
					df_comment_post = pd.concat(comment_posts_list,ignore_index=True)
					df_comment['pid'] = df_comment_post['id']

					# Get comment source match
					df_comment['source'] = df_comment['source'].apply(self.matchPostSource)

					df_list.append(df_comment)
					self.logger.info("Totally {} records in page {}".format(len(df_comment), page))

				except StopIteration:
					self.logger.info("Totally {} page(s)".format(page - 1))
					loop = False
			if not df_list:
				self.logger.warning("No data to update")
				return
			df_comment = pd.concat(df_list, ignore_index=True)
			# Upsert users before comments
			df_user_list = [user for user in df_comment['user']]
			df_user = pd.DataFrame(df_user_list)
			df_user_cleaned = self.cleanRecords(df_user,renameColumns={'id': 'uid'})
			self.upsertToDB(User,df_user_cleaned)

			df_comment_cleaned = self.cleanRecords(df_comment,dropColumns=['status','user'])
			self.upsertToDB(Comment,df_comment_cleaned)

			self.logger.info("Totally {} records in {} page(s)".format(len(df_comment_cleaned), page-1))
			return df_comment_cleaned

		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)

	async def getTagsBatchOther(self,uids):
		"""
		Documentation
		http://open.weibo.com/wiki/C/2/tags/tags_batch/other

		:param uids:
		:return:
		"""

		def f(x):
			"""
			:param x:{'id': 17, 'tags': [{'291511': 'Ree', 'weight': '52', 'flag': '0'},...]}
			:return:[{'tag_id': '291511','tag_name': 'Ree', 'weight': '52', 'flag': '0','id':17},...]
			"""
			tagId = x.pop('id')
			tagList = []

			for tag in x['tags']:
				tag['id'] = tagId
				dictTuple = list(tag.items())
				tagTuple = dictTuple.pop(0)
				tag_id = tagTuple[0]
				tag_name = tagTuple[1]
				dictTuple.append(('tag_id', tag_id))
				dictTuple.append(('tag_name', tag_name))
				tagList.append(dict(dictTuple))
			return tagList

		self.logger.info("Calling getTagsBatchOther function")
		try:
			paramsDict = {}
			paramsDict['uids'] = uids
			paramsDict['access_token'] = self.__apiToken
			url = 'https://c.api.weibo.com/2/tags/tags_batch/other.json'

			result = await self.getAsyncRequest(url, paramsDict)

			if not result:
				self.logger.warning("No data return!")
				return
				#raise Exception("No data return!")

			data = reduce(lambda x,y: x+y, map(f,result))
			df_tag = pd.DataFrame(data)
			df_tag_cleaned = self.cleanRecords(df_tag,renameColumns={'id':'uid'},utcTimeCovert=False)
			self.upsertToDB(UserTag,df_tag_cleaned)
			return df_tag_cleaned
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
			exit(1)

