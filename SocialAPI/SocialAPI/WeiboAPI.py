from pymongo import MongoClient, MongoReplicaSetClient
import asyncio
import uvloop
from .SocialBasicAPI import SocialBasicAPI
from SocialAPI.Helper import Helper
import sys, time
from datetime import datetime
from multiprocessing import Pool
import urllib
from json import JSONDecodeError
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
import re
import hashlib
from zipfile import ZipFile
import os
from SocialAPI.Model import WeiboSearchLimitedLastAttitude, WeiboKolLastAttitude,WeiboMentionLastAttitude
from SocialAPI.Model import WeiboKolLastComment,WeiboSearchLimitedLastComment,WeiboMentionLastComment
from SocialAPI.Model import WeiboSearchLimitedLastRepost,WeiboKolLastRepost

import pandas as pd

def doCommentParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.get_comments_show(*args,**kwargs)

def doAttitudeParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.get_attitudes_show(*args,**kwargs)

def doRepostParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.get_status_repost_timeline(*args,**kwargs)

def doMentionParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.get_statuses_mentions_other(*args,**kwargs)

class SocialWeiboAPI(SocialBasicAPI):

    def __init__(self):
        super(SocialWeiboAPI, self).__init__()
        self.__apiToken = self.cfp.get('api', 'weibo')
        self.__rootPath = Helper().getRootPath()
        self.__user = urllib.parse.quote_plus(self.cfp.get('mongodb_weibo', 'user'))
        self.__pwd = urllib.parse.quote_plus(self.cfp.get('mongodb_weibo', 'pwd'))
        self.__host = self.cfp.get('mongodb', 'host')
        self.__port = self.cfp.get('mongodb', 'port')

        self.__uri = 'mongodb://' + self.__user + ':' + self.__pwd + '@' + self.__host + ':' + self.__port + '/' + 'weibo'
        self.client = MongoClient(self.__uri)

    async def get_user_show_batch_other(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/show_batch/other

        :param uids: seperated by ',', max 50
        :return:
        """
        self.logger_access.info("Calling get_user_show_batch_other with uids: {}".format(uids))
        client = self.client
        db = client.weibo
        try:
            userTable = db.weibo_user_info

            params_dict = {'access_token': self.__apiToken, 'uids': uids}
            url = 'https://c.api.weibo.com/2/users/show_batch/other.json'

            result = await self.getAsyncRequest(url, params_dict)

            if result.get('error_code') is not None:
                raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
            users = result.get('users')
            if not users:
                raise Exception("No data returned for uids-{}".format(uids))


            update_operations = list()
            for user in users:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                op = UpdateOne({'id': user['id']},
                                          {'$set': user, '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                update_operations.append(op)

            userTable.bulk_write(update_operations,ordered=False,bypass_document_validation=False)

            self.logger_access.info('{} records have been updated for users {}'.format(len(users),uids))
            return
        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className':class_name,'functionName':function_name,'params':uids,'createdTime':datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),'msg':msg})

        finally:
            client.close()

    async def get_tags_batch_other(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/tags/tags_batch/other

        :param uids:
        :return:
        """

        self.logger_access.info("Calling get_tags_batch_other function with uids: {}".format(uids))
        client = self.client
        db = client.weibo
        userTable = db.weibo_user_tag
        # users = usersTable.insert_many(users)

        try:
            paramsDict = {}
            paramsDict['uids'] = uids
            paramsDict['access_token'] = self.__apiToken
            url = 'https://c.api.weibo.com/2/tags/tags_batch/other.json'

            result = await self.getAsyncRequest(url, paramsDict)

            if not result:
                self.logger_access.warning("No data returned for uids:{}".format(uids))
                return

            update_operations = list()
            for user in result:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                op = UpdateOne({'id': user['id']},
                                           {'$set': user,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, upsert=True)
                update_operations.append(op)

            userTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            self.logger_access.info('{} records have been updated for users {}'.format(len(result),uids))
            return

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uids,'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})

        finally:
            client.close()

    def statuses_show_batch_biz(self,ids,trim_user=1,isGetLongText=1):
        """
        Documentation
        https://open.weibo.com/wiki/C/2/statuses/show_batch/biz

        :param ids: mids, seperated by ','
        :param trim_user:
        :param isGetLongText:
        :return:
        """
        self.logger_access.info("Calling statuses_show_batch_biz with ids: {}".format(ids))
        client = self.client
        db = client.weibo
        postTable = db.weibo_user_post
        try:
            params_dict = {}
            params_dict['access_token'] = self.__apiToken
            params_dict['ids'] = ids
            params_dict['trim_user'] = 1# trim_user
            params_dict['isGetLongText'] = isGetLongText
            url = 'https://c.api.weibo.com/2/statuses/show_batch/biz.json'

            result = self.getRequest(url, params_dict)
            ret = result.json()
            if not ret:
                self.logger_access.warning('No data returned for mids - {}'.format(ids))
                return
            if ret.get('error_code') is not None:
                raise Exception('Error Code: {}, Error Msg: {}'.format(ret.get('error_code'), result.get('error')))
            posts = ret.get('statuses')

            update_operations = list()
            for post in posts:
                if post.get('created_at'):
                    post['created_at_timestamp'] = int(
                        time.mktime(time.strptime(post['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    post['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                       time.localtime(post['created_at_timestamp']))
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                op = UpdateOne({'id': post['id']},
                               {'$set': post,
                                '$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                               upsert=True)
                update_operations.append(op)

            postTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    def get_user_timeline_other(self,uid,page_start=1,page_range=5,page_limit=None,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/statuses/user_timeline/other

        :param kwargs:
        :return:
        """

        self.logger_access.info("Calling getStatusesUserTimelineOther with uid: {}".format(uid))

        client = self.client
        db = client.weibo
        postTable = db.weibo_user_post

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:
            page = page_start
            loop = True
            postList = list()

            params_dict = kwargs
            params_dict['access_token'] = self.__apiToken
            params_dict['uid'] = uid
            start_day = params_dict.get('start_day', -7)
            params_dict['trim_user'] = params_dict.get('trim_user', 1)
            params_dict['start_time'] = self.getTimeStamp(self.getStrTime(start_day))
            #params_dict['start_time'] = self.getTimeStamp('2018-01-01 00:00:00')
            #params_dict['end_time'] = self.getTimeStamp('2018-01-01 00:00:00')
            if params_dict.get('end_day'):
                end_day = params_dict.get('end_day')
                params_dict['end_time'] = self.getTimeStamp(self.getStrTime(end_day))
            url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'

            while loop:
                try:
                    
                    if page_limit and page >page_limit:
                        raise StopIteration
                    page += page_range
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,params_dict,page=i), loop=event_loop) for i in range(page-page_range,page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))

                    result = [] #[task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e :
                            self.logger_error.error(e)
                            pass

                    if not result:
                        raise StopIteration
                    for item in result:
                        if item.get('error_code') is not None:
                            raise Exception('Uid: {}, Error Code: {}, Error Msg: {}'.format(uid, item.get('error_code'), item.get('error')))
                        statuses = item.get('statuses')
                        if not statuses:
                            raise StopIteration
                        postList += statuses
                except StopIteration:
                    loop = False

            if not postList:
                self.logger_access.info('No post returned in last {} day(s) for user {}'.format(-start_day+1,uid))
                return

            update_operations = list()
            for post in postList:
                if post.get('created_at'):
                    post['created_at_timestamp'] = int(time.mktime(time.strptime(post['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    post['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(post['created_at_timestamp']))
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                op = UpdateOne({'id': post['id']},
                                           {'$set': post,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                update_operations.append(op)

            postTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            self.logger_access.info('{} records have been updated for user {}'.format(len(postList),uid))

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()
            event_loop.close()

    async def get_users_count_batch(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/counts_batch/other

        :param uids: seperated by ',', max 100
        :return:
        """
        self.logger_access.info("Calling get_users_count_batch with uids: {}".format(uids))

        client = self.client
        db = client.weibo
        userGrowthTable = db.weibo_user_growth

        try:

            params_dict = {'access_token': self.__apiToken, 'uids': uids}

            url = 'https://c.api.weibo.com/2/users/counts_batch/other.json'

            result = await self.getAsyncRequest(url, params_dict)

            if not result:
                self.logger_access.warning('No data returned for uids - {}'.format(uids))
                return

            update_operations = list()
            for user in result:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                op = UpdateOne({'id':user['id'],'createDay':str(datetime.now().date())},
                                             {'$set': user, '$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                             upsert=True)
                update_operations.append(op)

            userGrowthTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            self.logger_access.info('{} records have been updated for users {}'.format(len(result),uids))

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uids,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()


    def get_comments_show(self,mid,page_start=1,page_range=5,page_limit=20,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/comments/show/all
        :param mid:
        :param page_start
        :param page_range
        :param page_limit
        :param kwargs:
        :return:
        """
        self.logger_access.info("Calling get_comments_show function with mid: {}".format(mid))

        client = self.client
        db = client.weibo
        commentTable = db.weibo_user_comment
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:
            url = 'https://c.api.weibo.com/2/comments/show/all.json'
            page = page_start
            commentList = list()
            loop = True
            
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid

            while loop:
                try:
                    
                    if page_limit and page >page_limit:
                        raise StopIteration
                    page += page_range
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-page_range,page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))

                    result = []  # [task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e:
                            self.logger_error.error(e)
                            pass

                    for item in result:
                        if not item:
                            raise StopIteration
                        if item.get('error_code') is not None:
                            raise Exception('Post: {}, Error Code: {}, Error Msg: {}'.format(mid, item.get('error_code'), item.get('error')))
                        comments = item.get('comments')
                        if not comments:
                            raise StopIteration
                        commentList += comments


                except StopIteration:
                    loop = False

            if not commentList:
                self.logger_access.warning("No data to update for post {}".format(mid))
                return

            update_operations = list()
            for comment in commentList:
                comment['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if comment.get('created_at'):
                    comment['created_at_timestamp'] = int(time.mktime(time.strptime(comment['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    comment['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(comment['created_at_timestamp']))
                if comment.get('user'):
                    if not comment.get('user').get('european_user'):
                        comment['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(comment['user']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                if comment.get('status'):
                    comment['status']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(comment['status']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                #res = commentTable.update({'id':comment['id']},{'$set':comment,
                 #                                               '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                op = UpdateOne({'id':comment['id']},{'$set':comment,
                                                               '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                update_operations.append(op)

            #res = commentTable.insert_many(commentList)
            commentTable.bulk_write(update_operations,ordered=False,bypass_document_validation=False)
            self.logger_access.info('{} records have been inserted for post {}'.format(len(commentList),mid))

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': mid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()
            event_loop.close()


    def get_attitudes_show(self,mid,page_start=1,page_range=5,page_limit=20,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/attitudes/show/biz
        :param mid: mid is int64, but using async value has to be string or int
        :param latest:
        :param kwargs: count
        :return:
        """
        client = self.client
        db = client.weibo
        attitudeTable = db.weibo_user_attitude

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:
            url = 'https://c.api.weibo.com/2/attitudes/show/biz.json'
            page = page_start
            loop = True

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid


            if not attitudeTable.index_information():
                attitudeTable.create_index([('id', 1)],unique=True)

            self.logger_access.info("Calling get_attitudes_show function with mid: {}".format(mid))

            while loop:
                try:
                    attitudeList = list()
                    if page_limit and page > page_limit:
                        #print('Stop at page {}'.format(page))
                        raise StopIteration
                    page += page_range
                    self.logger_access.info('Running from page {} to page {}'.format(page-page_range,page-1))
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-page_range,page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))
                    result = list()  # [task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e:
                            self.logger_error.error(e)
                            pass

                    for item in result:
                        if not item:
                            raise StopIteration
                        if item.get('error_code') is not None:
                            raise Exception('Post: {}, Error Code: {}, Error Msg: {}'.format(mid, item.get('error_code'), item.get('error')))
                        attitudes = item.get('attitudes')
                        if not attitudes:
                            """
                            self.logger_error.error('Missing attitude items')
                            class_name = self.__class__.__name__
                            function_name = sys._getframe().f_code.co_name
                            msg = 'From page {} to page {} missing attitude items'.format(page-page_range,page-1)
                            db.weibo_error_log.insert(
                                {'className': class_name, 'functionName': function_name, 'params': mid,
                                 'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
                            continue
                            """
                            raise StopIteration
                        attitudeList += attitudes

                    if not attitudeList:
                        raise StopIteration

                    update_operations = list()
                    for attitude in attitudeList:
                        attitude['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        #attitude['createdTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if attitude.get('created_at'):
                            attitude['created_at_timestamp'] = int(
                                time.mktime(time.strptime(attitude['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                            attitude['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                   time.localtime(attitude['created_at_timestamp']))
                        if attitude.get('user'):
                            if not attitude.get('user').get('european_user'):
                                attitude['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
                                    attitude['user']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                        if attitude['status']:
                            attitude['status']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
                                attitude['status']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                        op = UpdateOne({'id':attitude['id']},{'$set':attitude,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},upsert=True)
                        update_operations.append(op)
                        """
                        res = attitudeTable.update({'id':attitude['id']},{'$set':attitude,
                                            '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},upsert=True)
                        """
                    attitudeTable.bulk_write(update_operations,ordered=False,bypass_document_validation=False)
                    self.logger_access.info('{} records has been inserted for post {}'.format(len(attitudeList), mid))
                except StopIteration:
                    loop = False

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': mid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()
            event_loop.close()

    def exportAttitudesShowUids(self,urls):
        myHelper = Helper()
        client = self.client
        db = client.weibo
        userTable = db.weibo_user_attitude_new

        for url in urls:
            matchObj = re.search(r'/(\w+)$',url)
            if matchObj:
                mid = matchObj.group(1)
            else:
                self.logger_error.error('URL is invalid - {}'.format(url))
            id = myHelper.convertMidtoId(mid)
            uids = userTable.find({'status.id':id},{'id':1,'user.id':1})
            filepath = self.__rootPath + '/output/weibo/attitude/' + str(id) +'.txt'

            with open(filepath,'w+') as f:
                for user in uids:
                    try:
                        uid = str(user['user']['id']) + '\n'
                        f.write(uid)
                    except Exception as e:
                        msg = e + ' - ' + str(user['id'])
                        print(msg)


    def get_status_repost_timeline(self,mid,page_start=1,page_range=5,page_limit=20,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/statuses/repost_timeline/all
        :param mid:
        :param kwargs:
        :return:
        """
        client = self.client
        db = client.weibo
        repostTable = db.weibo_user_repost2

        try:
            url = 'https://c.api.weibo.com/2/statuses/repost_timeline/all.json'
            page = page_start
            loop = True
            repostList = list()

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid



            self.logger_access.info('Calling get_status_repost_timeline function with mid: {}'.format(mid))

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:
                    page += page_range
                    if page_limit and page > page_limit:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-page_range,page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))
                    result = []  # [task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e:
                            self.logger_error.error(e)
                            pass

                    for item in result:
                        if not item:
                            raise StopIteration
                        if item.get('error_code') is not None:
                            raise Exception('Post: {}, Error Code: {}, Error Msg: {}'.format(mid, item.get('error_code'), item.get('error')))
                        reposts = item.get('reposts')
                        if not reposts:
                            raise StopIteration
                        repostList += reposts
                except StopIteration:
                    loop = False


            if not repostList:
                self.logger_access.warning("No data to update for post {}".format(mid))
                return
            for repost in repostList:
                repost['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                repost['createdTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if repost.get('created_at'):
                    repost['created_at_timestamp'] = int(time.mktime(time.strptime(repost['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    repost['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(repost['created_at_timestamp']))
                if repost.get('user'):
                    if not repost.get('user').get('european_user'):
                        repost['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(repost['user']['created_at'],"%a %b %d %H:%M:%S %z %Y"))
                if repost.get('retweeted_status'):
                    repost['retweeted_status']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(repost['retweeted_status']['created_at'],"%a %b %d %H:%M:%S %z %Y"))
                #res = attitudeTable.update({'id':attitude['id']},{'$set':attitude,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)


            res = repostTable.insert_many(repostList)
            self.logger_access.info('{} records has been inserted for post {}'.format(len(repostList),mid))


        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': mid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})

        finally:
            client.close()
            event_loop.close()

    def get_statuses_mentions_other(self, uid, page_start=1,page_range=5,page_limit=10,**kwargs):
        """
        Documentation


        :param uid:
        :param kwargs:
        :return:
        """
        #self.logger_access.info("Calling get_statuses_mentions_other function")

        client = self.client
        db = client.weibo
        mention_table = db.weibo_post_mention
        user_table = db.weibo_user_info

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:

            page = page_start
            loop = True

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['uid'] = uid

            url = 'https://c.api.weibo.com/2/statuses/mentions/other.json'

            while loop:
                try:
                    mentionList = list()

                    if page_limit and page > page_limit:
                        raise StopIteration
                    page += page_range
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url, paramsDict, page=i), loop=event_loop) for i
                             in range(page - page_range, page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))

                    result = []  # [task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e:
                            msg = '{} for user {}'.format(e,uid)
                            self.logger_error.error(msg)
                            pass

                    for item in result:
                        if not item:
                            raise StopIteration
                        if item.get('error_code') is not None:
                            raise Exception(
                                'User: {}, Error Code: {}, Error Msg: {}'.format(uid, item.get('error_code'),
                                                                                 item.get('error')))
                        mentions = item.get('statuses')
                        if not mentions:
                            #self.logger_access.info('No mentions for user {}'.format(uid))
                            raise StopIteration
                        mentionList += mentions
                except StopIteration:
                    loop = False

                update_operations_mention = list()
                update_operations_user = list()
                for mention in mentionList:
                    mention['uid_mentioned'] = uid
                    mention['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    if mention.get('created_at'):
                        mention['created_at_timestamp'] = int(
                            time.mktime(time.strptime(mention['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                        mention['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                              time.localtime(mention['created_at_timestamp']))

                    if mention.get('user'):
                        if not mention.get('user').get('european_user'):
                            mention['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
                                mention['user']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                            mention_user = mention['user']
                            mention_user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            op_user = UpdateOne({'id':mention_user['id']},
                                                {'$set':mention_user,
                                                 '$setOnInsert': {
                                                     'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                                                 }, upsert=True)
                            update_operations_user.append(op_user)

                    op_mention = UpdateOne({'id': mention['id'], 'uid_mentioned': mention['uid_mentioned']},
                                   {'$set': mention,
                                    '$setOnInsert': {
                                        'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                   upsert=True)

                    update_operations_mention.append(op_mention)

                if update_operations_user:
                    user_table.bulk_write(update_operations_user, ordered=False, bypass_document_validation=False)
                if update_operations_mention:
                    mention_table.bulk_write(update_operations_mention, ordered=False, bypass_document_validation=False)
                self.logger_access.info(
                    '{} records have been inserted for user {}'.format(len(mentionList), uid))

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()
            event_loop.close()

    def search_statuses_limited(self,start_time, end_time, q,page_start=1, page_range=20, page_limit=None,**kwargs):

        self.logger_access.info("Calling search_statuses_limited function for {} from {} to {}".format(q[1],start_time,end_time))

        client = self.client
        db = client.weibo
        search_table = db.weibo_search_statuses_limited
        user_table = db.weibo_user_info

        loop = True

        try:
            paramsDict = kwargs.copy()
            paramsDict['access_token'] = self.__apiToken
            paramsDict['starttime'] = self.getTimeStamp(start_time, 's')
            paramsDict['endtime'] = self.getTimeStamp(end_time, 's')
            paramsDict['q'] = q[1]
            page = page_start
            post_list = list()
            url = 'https://c.api.weibo.com/2/search/statuses/limited.json'

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()

            while loop:
                try:
                    
                    if page_limit and page > page_limit:
                        raise StopIteration
                    page += page_range
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url, paramsDict, page=i), loop=event_loop) for i
                             in range(page - page_range, page)]
                    event_loop.run_until_complete(asyncio.wait(tasks))

                    result = []  # [task.result() for task in tasks]
                    for task in tasks:
                        try:
                            result.append(task.result())
                        except JSONDecodeError as e:
                            self.logger_error.error(e)
                            pass

                    for item in result:
                        if not item:
                            raise StopIteration
                        if item.get('error_code') is not None:
                            raise Exception(
                                'Query: {}, Error Code: {}, Error Msg: {}'.format(q, item.get('error_code'),
                                                                                 item.get('error')))
                        posts = item.get('statuses')
                        if not posts:
                            raise StopIteration
                        post_list += posts

                except StopIteration:
                    loop = False

            update_operations_post = list()
            update_operations_user = list()
            for post in post_list:
                post['q_id'] = q[0]
                post['q'] = q[1]
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if post.get('created_at'):
                    post['created_at_timestamp'] = int(
                        time.mktime(time.strptime(post['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    post['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                          time.localtime(post['created_at_timestamp']))
                if post.get('user'):
                    if not post.get('user').get('european_user'):
                        post['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
                            post['user']['created_at'], "%a %b %d %H:%M:%S %z %Y"))
                        post_user = post['user']
                        post_user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        op_user = UpdateOne({'id': post_user['id']},
                                            {'$set': post_user,
                                             '$setOnInsert': {
                                                 'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                                             }, upsert=True)
                        update_operations_user.append(op_user)
                op = UpdateOne({'id': post['id']},
                               {'$set': post,
                                '$setOnInsert': {
                                    'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                               upsert=True)
                update_operations_post.append(op)

            if update_operations_user:
                user_table.bulk_write(update_operations_user, ordered=False, bypass_document_validation=False)
            if update_operations_post:
                search_table.bulk_write(update_operations_post, ordered=False, bypass_document_validation=False)
            #self.logger_access.info(str(res.bulk_api_result))
        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': paramsDict,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
        finally:
            client.close()
            event_loop.close()

    def search_statuses_history_create(self, starttime, endtime, **kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/create

        :param starttime: '%Y-%m-%d %H:%M:%S'
        :param endtime: '%Y-%m-%d %H:%M:%S'
        :param kwargs: q, province and ids at least on of the three is required
        :return:
        """
        self.logger_access.info("Calling search_statuses_history_create function")

        client = self.client
        db = client.weibo
        history_create_table = db.weibo_search_statuses_history_create

        try:
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['starttime'] = self.getTimeStamp(starttime, 'ms')
            paramsDict['endtime'] = self.getTimeStamp(endtime, 'ms')

            url = 'https://c.api.weibo.com/2/search/statuses/historical/create.json'

            result = self.postRequest(url, paramsDict)
            result = result.json()

            if result.get('error_code') is not None:
                raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
            result['status'] = False
            result['updatedTime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            history_create_table.update({'task_id': result['task_id']},{'$set': result,'$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},upsert = True)


            self.logger_access.info("Task {} is created.".format(result['task_id']))

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)
        finally:
            client.close()

    def search_statuses_history_check(self):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/check
        """
        self.logger_access.info("Calling search_statuses_history_check function")
        try:
            url = 'https://c.api.weibo.com/2/search/statuses/historical/check.json'
            finishedTasks = list()
            # Get task which is not downloaded yet
            client = self.client
            db = client.weibo
            history_create_table = db.weibo_search_statuses_history_create

            for task in history_create_table.find({'status':False}):
                timestamp = int(time.time() * 1000)
                taskId = task['task_id']
                id = task['id']
                secretKey = task['secret_key']
                q = task['q']
                pw = str(id) + secretKey + str(timestamp)
                paramsDict = {'access_token': self.__apiToken, 'task_id': taskId, 'timestamp': timestamp,
                              'signature': hashlib.md5(pw.encode('utf-8')).hexdigest()}
                result = self.getRequest(url, paramsDict)
                result = result.json()

                if result.get('error_code') is not None:
                    raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
                if result.get('status') is True:
                    self.search_statuses_history_download(task)
                    self.logger_access.info("Task {} is done and returns {} records".format(taskId, result.get('count')))

                    finishedTasks.append(result)

            if finishedTasks:
                try:
                    for task in finishedTasks:
                        task['updatedTime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
                        history_create_table.update({'task_id': task['task_id']}, {'$set': task, '$setOnInsert': {
                            'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}}, upsert=True)
                except Exception as e:
                    self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
                    exit(1)
                finally:
                    pass

            return finishedTasks

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)

    def search_statuses_history_download(self,task,chunkSize=1024,batchSize=10000):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/download
        """
        self.logger_access.info("Calling search_statuses_history_download function")
        client = self.client
        db = client.weibo
        historyTable = db.weibo_search_statuses_history_result

        try:
            if not historyTable.index_information():
                historyTable.create_index([('mid', 1)])

            url = 'https://c.api.weibo.com/2/search/statuses/historical/download.json'
            timestamp = int(time.time() * 1000)
            pw = str(task['id']) + task['secret_key'] + str(timestamp)
            paramsDict = {'access_token': self.__apiToken, 'task_id': task['task_id'], 'timestamp': timestamp,
                          'signature': hashlib.md5(pw.encode('utf-8')).hexdigest()}

            result = self.getRequest(url, paramsDict)

            if not result.ok:
                raise Exception('Error Code: {}, Error Msg: {}'.format(result.status_code, result.reason))

            downloadUrl = result.url

            r = self.getRequest(downloadUrl)

            # Download zip file
            localzipFile = self.__rootPath + '/output/{}.zip'.format(task['task_id'])
            with open(localzipFile, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunkSize):
                    f.write(chunk)

            fileInfo = os.stat(localzipFile)
            self.logger_access.info('{} is downloaded with {} bytes'.format(localzipFile, fileInfo.st_size))

            # Read log file and write into db
            update_operations = list()
            extractFile = '{}.log'.format(task['task_id'])
            with ZipFile(localzipFile) as myzip:
                pwd = str(task['task_id']) + task['secret_key']
                with myzip.open(extractFile, pwd=pwd.encode('utf-8')) as myfile:
                    for post in myfile:
                        post = eval(post.decode())
                        post['q'] = task['q']
                        # This is a bug of weibo api
                        post['blog_url'] = post['blog_url ']
                        post.pop('blog_url ')

                        op = UpdateOne({'mid': post['mid']},
                                   {'$set': post,
                                    #'$rename':{'blog_url ':'blog_url'},
                                    '$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                                    },
                                   upsert=True
                               )
                        update_operations.append(op)
                        if len(update_operations) == batchSize:
                            historyTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
                            update_operations.clear()
                    if update_operations:
                        historyTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
                        update_operations.clear()

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)

        finally:
            client.close()

    def get_comment_by_since_id(self,mid):
        ret = self.__check_comment_since_id(mid)
        if ret:
            for _ in ret:
                self.get_comments_show(mid=_['id'],since_id=_['since_id'])

    def get_attitude_by_since_id(self,mid):
        ret = self.__check_attitude_since_id(mid)
        if ret:
            for _ in ret:
                self.get_attitudes_show(mid=_['id'], since_id=_['since_id'])

    def get_repost_by_since_id(self,mid):
        ret = self.__check_repost_since_id(mid)
        if ret:
            for _ in ret:
                self.get_status_repost_timeline(mid=_['id'],since_id=_['since_id'])

    def __check_repost_since_id(self,mid):

        session = self.createSession()
        try:

            # Get post IDs from search limited for repost
            pids = session.query(WeiboSearchLimitedLastRepost.pid, WeiboSearchLimitedLastRepost.since_id) \
                .filter(WeiboSearchLimitedLastRepost.pid == mid) \
                .all()

            repostPostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Get post IDs from post daily for repost
            pids = session.query(WeiboKolLastRepost.pid, WeiboKolLastRepost.since_id) \
                .filter(WeiboKolLastRepost.pid == mid) \
                .all()

            repostPostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]
            repostPostList += repostPostListFromKOL

            if not repostPostList:
                return None
            df = pd.DataFrame(repostPostList)
            df = df.sort_values(by='since_id', ascending=False)
            df = df.drop_duplicates(subset='id', keep='first')
            return df.to_dict('records')

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)
        finally:
            session.close()

    def __check_comment_since_id(self,mid):

        session = self.createSession()
        try:
            # Get post IDs from search limited for comment
            pids = session.query(WeiboSearchLimitedLastComment.pid, WeiboSearchLimitedLastComment.since_id) \
                .filter(WeiboSearchLimitedLastComment.pid == mid) \
                .all()
            commentPostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Get post IDs from post daily for comment
            pids = session.query(WeiboKolLastComment.pid, WeiboKolLastComment.since_id) \
                .filter(WeiboKolLastComment.pid == mid) \
                .all()
            commentPostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Get post IDs from mention for comment
            pids = session.query(WeiboMentionLastComment.pid, WeiboMentionLastComment.since_id) \
                .filter(WeiboMentionLastComment.pid == mid) \
                .all()
            commentPostListFromMetion = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Merge daily, kol, mention pid
            commentPostList += commentPostListFromKOL
            commentPostList += commentPostListFromMetion

            if not commentPostList:
                return None
            df = pd.DataFrame(commentPostList)
            df = df.sort_values(by='since_id', ascending=False)
            df = df.drop_duplicates(subset='id', keep='first')
            return df.to_dict('records')

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)
        finally:
            session.close()

    def __check_attitude_since_id(self,mid):
        session = self.createSession()
        try:
            pids = session.query(WeiboSearchLimitedLastAttitude.pid, WeiboSearchLimitedLastAttitude.since_id) \
                .filter(WeiboSearchLimitedLastAttitude.pid == mid) \
                .all()
            attitudePostList = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Get post IDs from post daily for attitude
            pids = session.query(WeiboKolLastAttitude.pid, WeiboKolLastAttitude.since_id) \
                .filter(WeiboKolLastAttitude.pid == mid) \
                .all()
            attitudePostListFromKOL = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Get post IDs from mention for attitude
            pids = session.query(WeiboMentionLastAttitude.pid, WeiboMentionLastAttitude.since_id) \
                .filter(WeiboMentionLastAttitude.pid == mid) \
                .all()
            attitudePostListFromMention = [{'id': _[0], 'since_id': _[1]} for _ in pids]

            # Merge daily, kol, mention pid
            attitudePostList += attitudePostListFromKOL
            attitudePostList += attitudePostListFromMention

            if not attitudePostList:
                return None
            df = pd.DataFrame(attitudePostList)
            df = df.sort_values(by='since_id', ascending=False)
            df = df.drop_duplicates(subset='id', keep='first')
            return df.to_dict('records')

        except Exception as e:
            self.logger_error.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e))
            exit(1)
        finally:
            session.close()

    def doParallel(self,funcName,dataSet):
        try:
            p = Pool(3)
            result = []

            for i, item in enumerate(dataSet):
                if funcName == 'comment':
                    result.append(p.apply_async(func=doCommentParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=100)))
                elif funcName == 'attitude':
                    result.append(p.apply_async(func=doAttitudeParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=100)))
                elif funcName == 'repost':
                    result.append(p.apply_async(func=doRepostParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=50)))
                elif funcName == 'mention':
                    result.append(p.apply_async(func=doMentionParellelWrapper,args=(int(item['uid']),),kwds=dict(since_id=int(item['since_id']),count=200,filter_by_type=1)))

            p.close()
            p.join()
        except Exception as e:
            print(e)

    def __str__(self):
        return "Weibo APIs"
