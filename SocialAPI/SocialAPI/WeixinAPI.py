from pymongo import MongoClient
import asyncio
import uvloop
from .SocialBasicAPI import SocialBasicAPI
from SocialAPI.Helper import Helper
import sys, time
import json
from datetime import datetime
from multiprocessing import Pool


class SocialWeixinAPI(SocialBasicAPI):

    def __init__(self):
        super(SocialWeixinAPI, self).__init__()
        self.__apiToken = self.cfp.get('api', 'weibo')
        self.__rootPath = Helper().getRootPath()
        self._client = MongoClient()

    def getAccessTokenFromController(self,appid,appkey):
        try:
            url_ackey = 'http://api.woaap.com/api/ackey'
            paramsDict = {'appid':appid,'appkey':appkey}

            r = self.getRequest(url_ackey,paramsDict)
            res = r.json()
            if res.get('errcode') != 0:
                raise Exception(res.get('errmsg'))
            ackey = res['ackey']

            url_token = 'http://api.woaap.com/api/accesstoken'
            paramsDict = {'ackey': ackey}

            r = self.getRequest(url_token,paramsDict)
            res = r.json()
            access_token = res['access_token']

            return access_token


        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)

    def getUserCumulate(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getusercumulate?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getUserCimulate API for account {} from {} to {}'.format(account_name,begin_date,end_date))
            client = self._client
            db = client.weixin
            userTable = db.weixin_user_cumulate

            r = self.postRequest(url, postData)
            res = r.json()
            if res.get('errcode'):
                raise Exception(res.get('errmsg'))
            for user in res['list']:
                user['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                user['account_name'] = account_name
                result = userTable.update({'account_name': user['account_name'],'ref_date':user['ref_date'],'user_source':user['user_source']},
                                          {'$set': user, '$setOnInsert': {
                                              'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                          upsert=True)
            return

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': uids,'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    def getArticleSummary(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getarticlesummary?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getArticleSummary API for account {} from {} to {}'.format(account_name, begin_date, end_date))
            r = self.postRequest(url, postData)
            res = r.json()
            if res.get('errcode'):
                raise Exception(res.get('errmsg'))
            return res

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)

    def getUpstreamMsg(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getupstreammsg?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getUpstreamMsg API for account {} from {} to {}'.format(account_name, begin_date, end_date))
            client = self._client
            db = client.weixin
            msgTable = db.weixin_upstream_msg
            r = self.postRequest(url, postData)
            res = r.json()
            if res.get('errcode'):
                raise Exception(res.get('errmsg'))
            for account in res['list']:
                account['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                account['account_name'] = account_name
                result = msgTable.update({'account_name': account['account_name'],'ref_date':account['ref_date'],
                                          'user_source':account['user_source'],'msg_type':account['msg_type']},
                                          {'$set': account, '$setOnInsert': {
                                              'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                          upsert=True)
            return

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            self.logger.error(msg)
        finally:
            client.close()

    def __str__(self):
        return 'Social API of Weixin'
