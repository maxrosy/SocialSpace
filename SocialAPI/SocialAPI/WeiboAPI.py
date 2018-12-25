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
        #self.client = MongoClient()


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


            # users = usersTable.insert_many(users)
            for user in users:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                result = userTable.update({'id': user['id']},
                                          {'$set': user, '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                #self.logger_access.info('User {}: {} '.format(user['id'], result))
            self.logger_access.info('{} records have been updated for users {}'.format(len(users),uids))
            return

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className':class_name,'functionName':function_name,'params':uids,'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'msg':msg})
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
                self.logger.warning("No data returned for uids:{}".format(uids))
                return

            client = MongoClient()
            db = client.weibo
            usersTable = db.weibo_user_tag
            # users = usersTable.insert_many(users)
            for user in result:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                result = usersTable.update({'id': user['id']},
                                           {'$set': user,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}, upsert=True)
                #self.logger_access.info('User {}: {} '.format(user['id'], result))
            self.logger_access.info('{} records have been updated for users {}'.format(len(result),uids))
            return

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uids,'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    def getUserTimelineOther(self,uid,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/statuses/user_timeline/other

        :param kwargs:
        :return:
        """

        self.logger_access.info("Calling getStatusesUserTimelineOther with uid: {}".format(uid))

        try:

            page = 0
            loop = True
            postList = []

            params_dict = kwargs
            params_dict['access_token'] = self.__apiToken
            params_dict['uid'] = uid
            start_day = params_dict.get('start_day', -7)
            params_dict['trim_user'] = params_dict.get('trim_user', 1)
            params_dict['start_time'] = self.getTimeStamp(self.getStrTime(start_day))
            #params_dict['start_time'] = self.getTimeStamp('2018-01-01 00:00:00')
            #params_dict['end_time'] = self.getTimeStamp('2018-01-01 00:00:00')
            if params_dict.get('end_day'):
                params_dict['end_time'] = self.getTimeStamp(self.getStrTime(end_day))
            url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'


            client = self.client
            db = client.weibo
            postTable = db.weibo_user_post

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:
                    page += 5

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
                            raise Exception('Error Code: {}, Error Msg: {}'.format(item.get('error_code'), item.get('error')))
                        statuses = item.get('statuses')
                        if not statuses:
                            raise StopIteration
                        postList += statuses
                except StopIteration:
                    loop = False

            event_loop.close()
            if not postList:
                self.logger_access.info('No post returned in last {} day(s) for user {}'.format(-start_day+1,uid))
                return

            for post in postList:
                if post.get('created_at'):
                    post['created_at_timestamp'] = int(time.mktime(time.strptime(post['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    post['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(post['created_at_timestamp']))
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                res = postTable.update({'id': post['id']},
                                           {'$set': post,'$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
                #self.logger_access.info('Post {}: {} '.format(post['id'], res))
            self.logger_access.info('{} records have been updated for user {}'.format(len(postList),uid))

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    async def getUsersCountBatch(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/counts_batch/other

        :param uids: seperated by ',', max 100
        :return:
        """
        self.logger_access.info("Calling getUsersCountBatch with uids: {}".format(uids))
        try:
            client = self.client
            db = client.weibo
            userGrowthTable = db.weibo_user_growth

            params_dict = {'access_token': self.__apiToken, 'uids': uids}

            url = 'https://c.api.weibo.com/2/users/counts_batch/other.json'

            result = await self.getAsyncRequest(url, params_dict)

            if not result:
                self.logger.warning('No data returned for uids - {}'.format(uids))
                return

            for user in result:
                if user.get('created_at'):
                    user['created_at_timestamp'] = int(time.mktime(time.strptime(user['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    user['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(user['created_at_timestamp']))
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                res = userGrowthTable.update({'id':user['id'],'createDay':str(datetime.now().date())},
                                             {'$set': user, '$setOnInsert': {'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                             upsert=True)
                #self.logger_access.info('User {}: {} '.format(user['id'], res))
            self.logger_access.info('{} records have been updated for users {}'.format(len(result),uids))

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uids,
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()


    def getCommentsShow(self,mid,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/comments/show/all
        :param id:
        :param kwargs:
        :return:
        """
        self.logger_access.info("Calling getCommentsShow function with mid: {}".format(mid))
        try:
            url = 'https://c.api.weibo.com/2/comments/show/all.json'
            page = 0
            commentList = []
            loop = True
            
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid

            client = self.client
            db = client.weibo
            commentTable = db.weibo_user_comment

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:


                    page += 5

                    if page >20:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i+1), loop=event_loop) for i in range(page-5,page)]
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
                            raise Exception('Error Code: {}, Error Msg: {}'.format(item.get('error_code'), item.get('error')))
                        comments = item.get('comments')
                        if not comments:
                            raise StopIteration
                        commentList += comments


                except StopIteration:
                    loop = False

            event_loop.close()

            if not commentList:
                self.logger.warning("No data to update for post {}".format(mid))
                return


            for comment in commentList:
                comment['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                comment['createdTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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

            res = commentTable.insert_many(commentList)
            self.logger_access.info('{} records have been inserted for post {}'.format(len(commentList),mid))

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': mid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})


    def getAttitudesShow(self,mid,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/attitudes/show/biz
        :param mid: mid is int64, but using async value has to be string or int
        :param latest:
        :param kwargs: count
        :return:
        """

        try:
            url = 'https://c.api.weibo.com/2/attitudes/show/biz.json'
            page = 0
            loop = True
            attitudeList = []
    
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid

            client = self.client
            db = client.weibo
            attitudeTable = db.weibo_user_attitude

            self.logger_access.info("Calling getAttitudesShow function with mid: {}".format(mid))
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:
                    page += 4
                    if page > 20:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i+1), loop=event_loop) for i in range(page-4,page)]
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
                            raise Exception('Error Code: {}, Error Msg: {}'.format(item.get('error_code'), item.get('error')))
                        attitudes = item.get('attitudes')
                        if not attitudes:
                            raise StopIteration
                        attitudeList += attitudes
                except StopIteration:
                    loop = False
            event_loop.close()
            if not attitudeList:
                self.logger.warning("No data to update for post {}".format(mid))
                return

            for attitude in attitudeList:
                attitude['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                attitude['createdTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if attitude.get('created_at'):
                    attitude['created_at_timestamp'] = int(time.mktime(time.strptime(attitude['created_at'], "%a %b %d %H:%M:%S %z %Y")))
                    attitude['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(attitude['created_at_timestamp']))
                if attitude.get('user'):
                    if not attitude.get('user').get('european_user'):
                        attitude['user']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(attitude['user']['created_at'],"%a %b %d %H:%M:%S %z %Y"))
                if attitude['status']:
                    attitude['status']['created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',time.strptime(attitude['status']['created_at'],"%a %b %d %H:%M:%S %z %Y"))
                #res = attitudeTable.update({'id':attitude['id']},{'$set':attitude,
                #                                                   '$setOnInsert':{'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},upsert=True)
            res = attitudeTable.insert_many(attitudeList)
            self.logger_access.info('{} records has been inserted for post {}'.format(len(attitudeList),mid))
            #self.logger_access.info('Attitude {}: {}'.format(attitude['id'], res))

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger_error.error(msg)
            db.weibo_error_log.insert({'className': class_name, 'functionName': function_name, 'params': mid,
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})

    def getStatusRepostTimeline(self,mid,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/statuses/repost_timeline/all
        :param mid:
        :param latest:
        :param kwargs:
        :return:
        """

        try:
            url = 'https://c.api.weibo.com/2/statuses/repost_timeline/all.json'
            page = 0
            loop = True
            repostList = []

            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid

            client = self.client
            db = client.weibo
            repostTable = db.weibo_user_repost2

            self.logger_access.info('Calling getStatusRepostTimeline function with mid: {}'.format(mid))

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            event_loop = asyncio.new_event_loop()
            while loop:
                try:
                    page += 4
                    if page > 20:
                        raise StopIteration

                    tasks = [asyncio.ensure_future(self.getAsyncRequest(url,paramsDict,page=i+1), loop=event_loop) for i in range(page-4,page)]
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
                            raise Exception('Error Code: {}, Error Msg: {}'.format(item.get('error_code'), item.get('error')))
                        reposts = item.get('reposts')
                        if not reposts:
                            raise StopIteration
                        repostList += reposts
                except StopIteration:
                    loop = False
            event_loop.close()

            if not repostList:
                self.logger.warning("No data to update for post {}".format(mid))
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
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})


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

