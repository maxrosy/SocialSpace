from pymongo import MongoClient
import asyncio
import uvloop
from .SocialBasicAPI import SocialBasicAPI
from SocialAPI.Helper import Helper
import sys, time
from datetime import datetime

class SocialWeiboAPI(SocialBasicAPI):

    def __init__(self):
        super(SocialWeiboAPI, self).__init__()
        self.__apiToken = self.cfp.get('api', 'weibo')
        self.__rootPath = Helper().getRootPath()

    async def getUserShowBatchOther(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/show_batch/other

        :param uids: seperated by ',', max 50
        :return:
        """
        self.logger.info("Calling getUserShowBatchOther with uids: {}".format(uids))
        try:
            params_dict = {'access_token': self.__apiToken, 'uids': uids}
            url = 'https://c.api.weibo.com/2/users/show_batch/other.json'

            result = await self.getAsyncRequest(url, params_dict)

            if result.get('error_code') is not None:
                raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))
            users = result.get('users')
            if not users:
                raise Exception("No data returned for uids-{}".format(uids))

            client = MongoClient()
            db = client.weibo
            userTable = db.weibo_user_info
            # users = usersTable.insert_many(users)
            for user in users:
                user['updatedTime'] = int(time.time())
                result = userTable.update({'id': user['id']}, {'$set': user, '$setOnInsert':{'createdTime':int(time.time())}},upsert=True)
                self.logger.info('User {} : {} '.format(user['id'], result))
            
            return

        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'msg':msg})
        finally:
            client.close()

    async def getTagsBatchOther(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/tags/tags_batch/other

        :param uids:
        :return:
        """

        self.logger.info("Calling getTagsBatchOther function with uids: {}".format(uids))
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
                user['updatedTime'] = int(time.time())
                result = usersTable.update({'id': user['id']}, {'$set': user,'$setOnInsert':{'createdTime':int(time.time())}}, upsert=True)
                self.logger.info('User {} : {} '.format(user['id'], result))

            return

        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    def getUserTimelineOther(self, uid, **kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/statuses/user_timeline/other

        :param kwargs:
        :return:
        """

        self.logger.info("Calling getStatusesUserTimelineOther with uid: {}".format(uid))

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
            # params_dict['end_time'] = self.getTimeStamp('2018-01-15 00:00:00')
            url = 'https://c.api.weibo.com/2/statuses/user_timeline/other.json'


            client = MongoClient()
            db = client.weibo
            postTable = db.weibo_user_post

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
                    postList.append(statuses)
                except StopIteration:
                    self.logger.debug("Totally {} page(s)".format(page - 1))
                    loop = False

            if not postList:
                self.logger.warning('No post returned in last {} day(s) for user {}'.format(-start_day,uid))
                return
            for posts in postList:
                for post in posts:
                    post['updatedTime'] = int(time.time())
                    res = postTable.update({'id': post['id']}, {'$set': post,'$setOnInsert':{'createdTime':int(time.time())}},upsert=True)
                    self.logger.info('Post {} : {} '.format(post['id'], res))


        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime':datetime.now().strftime('%Y-%m-%d %H:%M:%S'),'msg':msg})
        finally:
            client.close()

    async def getUsersCountBatch(self, uids):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/users/counts_batch/other

        :param uids: seperated by ',', max 100
        :return:
        """
        self.logger.info("Calling getUsersCountBatch with uids: {}".format(uids))
        try:
            client = MongoClient()
            db = client.weibo
            userGrowthTable = db.weibo_user_growth

            params_dict = {'access_token': self.__apiToken, 'uids': uids}

            url = 'https://c.api.weibo.com/2/users/counts_batch/other.json'

            result = await self.getAsyncRequest(url, params_dict)

            if not result:
                self.logger.warning('No data returned for uids - {}'.format(uids))
                return

            for user in result:
                user['updatedTime'] = int(time.time())
                res = userGrowthTable.update({'id':user['id'],'createDay':str(datetime.now().date())},
                                             {'$set': user, '$setOnInsert': {'createdTime': int(time.time())}},
                                             upsert=True)
                self.logger.info('User {} : {} '.format(user['id'], res))

        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})

        finally:
            client.close()

    def getCommentsShow(self,mid,latest=True,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/comments/show/all
        :param id:
        :param kwargs:
        :return:
        """
        self.logger.info("Calling getCommentsShow function with mid: {}".format(mid))
        try:
            url = 'https://c.api.weibo.com/2/comments/show/all.json'
            page = 0
            resultList = []
            loop = True
            
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid
            
            client = MongoCient()
            db = client.weibo
            commentTable = db.weibo_user_comment
            
            if latest:
                since_id = commentTable.find().sort({'id':-1}).limit(1)
                if since_id:
                    paramsDict['since_id'] = since_id
            
            while loop:
                try:
                    page += 1
                    paramsDict['page'] = page
                    result = self.getRequest(url, paramsDict)
                    result = result.json()

                    if result.get('error_code') is not None:
                        raise Exception('Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

                    comments = result.get('comments')
                    if not comments or page == 21: # Since there are too many comments, stop after 10 pages to call DB upsert
                        raise StopIteration

                    resultList.append(comments)
                    
                except StopIteration:
                    self.logger.info("Totally {} page(s)".format(page - 1))
                    loop = False
                    
            if not resultList:
                self.logger.warning("No data to update for post {}".format(mid))
                return
            
            for comments in resultList:
                for comment in comments:
                    comment['updatedTime'] = int(time.time())
                    res = commentTable.update({'id':comment['id]},{'$set':comment, '$setOnInsert':{'createdTime':int(time.time())}},upsert=True)
                    print('Comment {} : {}'.format(comment['id'], res))

        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        
        finally:
            client.close()

    def getAttitudesShow(self,mid,latest=True,**kwargs):
        """
        Documentation
        http://open.weibo.com/wiki/C/2/attitudes/show/biz
        :param mid:
        :param latest:
        :param kwargs: count
        :return:
        """
        self.logger.info("Calling getAttitudesShow function with mid: {}".format(mid))
        try:
            url = 'https://c.api.weibo.com/2/attitudes/show/biz.json'
            page = 0
            loop = True
            resultList = []
    
            paramsDict = kwargs
            paramsDict['access_token'] = self.__apiToken
            paramsDict['id'] = mid
            
            client = MongoCient()
            db = client.weibo
            AttitudeTable = db.weibo_user_attitude
            
            if latest:
                since_id = attitudeTable.find().sort({'id':-1}).limit(1)
                if since_id:
                    paramsDict['since_id'] = since_id

            while loop:
                try:
                    page += 1
                    paramsDict['page'] = page
                    result = self.getRequest(url, paramsDict).json()

                    if result.get('error_code') is not None:
                        if result.get('error_code') == 20101:
                            self.logger.warning("Post {} has been removed!".format(mid))
                        else:
                            raise Exception(
                                'Error Code: {}, Error Msg: {}'.format(result.get('error_code'), result.get('error')))

                    attitudes = result.get('attitudes')
                    if not attitudes or page == 21:
                        raise StopIteration
                    resultList.append(attitudes)

                except StopIteration:
                    self.logger.info("Totally {} page(s)".format(page - 1))
                    loop = False

            if not resultList:
                self.logger.warning("No data to update for post {}".format(mid))
                return
                
            for result in resultList:
                for attitude in result:
                    attitude['updatedTime'] = int(time.time())
                    res = attitudeTable.update({'id':attitude['id]},{'$set':attitude, '$setOnInsert':{'createdTime':int(time.time())},upsert=True)
                    print('Attitude {}: {}'.format(attitude['id'], res))
            
            
        except Exception as e:
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weibo_error_log.insert({'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            
        finally:
            client.close()
