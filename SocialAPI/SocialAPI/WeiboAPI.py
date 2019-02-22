from pymongo import MongoClient
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

def doCommentParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.getCommentsShow(*args,**kwargs)

def doAttitudeParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.getAttitudesShow(*args,**kwargs)

def doRepostParellelWrapper(*args,**kwargs):
    w = SocialWeiboAPI()
    return w.getStatusRepostTimeline(*args,**kwargs)


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

    async def getUserShowBatchOther(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/show_batch/other

        :param uids: seperated by ',', max 50
        :return:
        """
        self.logger_access.info("Calling getUserShowBatchOther with uids: {}".format(uids))
        try:
            client = self.client
            db = client.weibo
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

    async def getTagsBatchOther(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/tags/tags_batch/other

        :param uids:
        :return:
        """

        self.logger_access.info("Calling getTagsBatchOther function with uids: {}".format(uids))
        try:
            paramsDict = {}
            paramsDict['uids'] = uids
            paramsDict['access_token'] = self.__apiToken
            url = 'https://c.api.weibo.com/2/tags/tags_batch/other.json'

            result = await self.getAsyncRequest(url, paramsDict)

            if not result:
                self.logger_access.warning("No data returned for uids:{}".format(uids))
                return

            client = MongoClient()
            db = client.weibo
            userTable = db.weibo_user_tag
            # users = usersTable.insert_many(users)

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

    def getUserTimelineOther(self,uid,startpage=0,pageRange=5,**kwargs):
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
            page = startpage
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
                    page += pageRange

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,params_dict,page=i+1), loop=event_loop) for i in range(page-5,page)]
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

    async def getUsersCountBatch(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/counts_batch/other

        :param uids: seperated by ',', max 100
        :return:
        """
        self.logger_access.info("Calling getUsersCountBatch with uids: {}".format(uids))

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


    def getCommentsShow(self,mid,startPage=1,pageRange=5,pageLimit=20,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/comments/show/all
        :param mid:
        :param startPage
        :param pageRange
        :param pageLimit
        :param kwargs:
        :return:
        """
        self.logger_access.info("Calling getCommentsShow function with mid: {}".format(mid))

        client = self.client
        db = client.weibo
        commentTable = db.weibo_user_comment_davidbackham
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:
            url = 'https://c.api.weibo.com/2/comments/show/all.json'
            page = startPage
            commentList = list()
            loop = True
            
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid

            while loop:
                try:
                    page += pageRange
                    if pageLimit and page >pageLimit:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-pageRange,page)]
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


    def getAttitudesShow(self,mid,pageStart=1,pageRange=5,pageLimit=20,**kwargs):
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
        attitudeTable = db.weibo_user_attitude_new

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:
            url = 'https://c.api.weibo.com/2/attitudes/show/biz.json'
            page = pageStart
            loop = True

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid


            if not attitudeTable.index_information():
                attitudeTable.create_index([('id', 1)],unique=True)

            self.logger_access.info("Calling getAttitudesShow function with mid: {}".format(mid))

            while loop:
                try:
                    attitudeList = list()
                    if pageLimit and page > pageLimit:
                        #print('Stop at page {}'.format(page))
                        raise StopIteration
                    page += pageRange
                    self.logger_access.info('Running from page {} to page {}'.format(page-pageRange,page-1))
                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-pageRange,page)]
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
                            self.logger_error.error('Missing attitude items')
                            class_name = self.__class__.__name__
                            function_name = sys._getframe().f_code.co_name
                            msg = 'From page {} to page {} missing attitude items'.format(page-pageRange,page-1)
                            db.weibo_error_log.insert(
                                {'className': class_name, 'functionName': function_name, 'params': mid,
                                 'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
                            continue
                            #raise StopIteration
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
                        op = UpdateOne({'id':attitude['id']},{'$set':attitude,
                                            '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}},upsert=True)
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


    def getStatusRepostTimeline(self,mid,pageStart=1,pageRange=5,pageLimit=20,**kwargs):
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
            page = pageStart
            loop = True
            repostList = list()

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid



            self.logger_access.info('Calling getStatusRepostTimeline function with mid: {}'.format(mid))

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:
                    page += pageRange
                    if pageLimit and page > pageLimit:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i), loop=event_loop) for i in range(page-pageRange,page)]
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

    def getStatusesMentionsOther(self, uid, pageStart=1,pageRange=5,pageLimit=20,**kwargs):
        """
        Documentaion
        https://open.weibo.com/wiki/C/2/statuses/mentions/other

        :param uid:
        :param kwargs:
        :return:
        """
        self.logger_access.info("Calling searchStatusesHistoryCreate function")

        client = self.client
        db = client.weibo
        mentionTable = db.weibo_post_mentions

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        event_loop = asyncio.new_event_loop()

        try:

            page = pageStart
            loop = True
            mentionList = list()

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['uid'] = uid

            url = 'https://c.api.weibo.com/2/statuses/mentions/other.json'

            while loop:
                try:
                    page += pageRange
                    if pageLimit and page > pageLimit:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url, paramsDict, page=i), loop=event_loop) for i
                             in range(page - pageRange, page)]
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
                                'User: {}, Error Code: {}, Error Msg: {}'.format(uid, item.get('error_code'),
                                                                                 item.get('error')))
                        mentions = item.get('statuses')
                        if not mentions:
                            raise StopIteration
                        mentionList += mentions

                except StopIteration:
                    loop = False


            if not mentionList:
                self.logger_access.warning("No data to update for user {}".format(uid))
                return
            update_operations = list()
            for mention in mentionList:
                mention['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if mention.get('created_at'):
                    mention['created_at_timestamp'] = int(
                        time.mktime(time.strptime(mention['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    mention['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                          time.localtime(mention['created_at_timestamp']))
                if mention.get('user'):
                    if not mention.get('user').get('european_user'):
                        mention['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(mention['user']['created_at'], "%a %b %d %H:%M:%S %z %Y"))

                op = UpdateOne({'id': mention['id']}, {'$set': mention,
                                                       '$setOnInsert': {
                                                           'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                update_operations.append(op)

            mentionTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            self.logger_access.info('{} records have been inserted for user {}'.format(len(mentionList), uid))

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

    def searchStatusesHistoryCreate(self, starttime, endtime, **kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/create

        :param starttime: '%Y-%m-%d %H:%M:%S'
        :param endtime: '%Y-%m-%d %H:%M:%S'
        :param kwargs: q, province and ids at least on of the three is required
        :return:
        """
        self.logger_access.info("Calling searchStatusesHistoryCreate function")

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

    def searchStatusesHistoryCheck(self):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/check
        """
        self.logger_access.info("Calling searchStatusesHistoryCheck function")
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
                    self.searchStatusesHistoryDownload(task)
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

    def searchStatusesHistoryDownload(self,task,chunkSize=1024,batchSize=10000):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/search/statuses/historical/download
        """
        self.logger_access.info("Calling searchStatusesHistoryDownload function")
        client = self.client
        db = client.weibo
        historyTable = db.weibo_search_statuses_history_result_2

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

    def doParallel(self,funcName,dataSet):
        try:
            p = Pool(3)
            result = []

            for i, item in enumerate(dataSet):
                if funcName == 'comment':
                    result.append(p.apply_async(func=doCommentParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=200)))
                elif funcName == 'attitude':
                    result.append(p.apply_async(func=doAttitudeParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=100)))
                elif funcName == 'repost':
                    result.append(p.apply_async(func=doRepostParellelWrapper,args=(str(int(item['id'])),),kwds=dict(since_id=str(int(item['since_id'])),count=50)))

            p.close()
            p.join()
        except Exception as e:
            print(e)

