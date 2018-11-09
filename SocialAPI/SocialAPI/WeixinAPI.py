from pymongo import MongoClient
import asyncio
import uvloop
from .SocialBasicAPI import SocialBasicAPI
from SocialAPI.Helper import Helper
import sys, time
import json
from datetime import datetime
from multiprocessing import Pool
import urllib
import redis

class SocialWeixinAPI(SocialBasicAPI):

    def __init__(self):
        super(SocialWeixinAPI, self).__init__()
        self.__apiToken = self.cfp.get('api', 'weibo')
        self.__rootPath = Helper().getRootPath()
        self.__mongo_user = urllib.parse.quote_plus(self.cfp.get('mongodb_weixin','user'))
        self.__mongo_pwd = urllib.parse.quote_plus(self.cfp.get('mongodb_weixin','pwd'))
        self.__mongo_host = self.cfp.get('mongodb','host')
        self.__mongo_port = self.cfp.get('mongodb','port')
        self.__redis_host = self.cfp.get('redis','host')
        self.__redis_port = self.cfp.get('redis','port')
        self.__redis_db = self.cfp.get('redis','db')
        self.__redisPool = redis.ConnectionPool(host=self.__redis_host, port=self.__redis_port, db=self.__redis_db)
        self.r = redis.Redis(connection_pool=self.__redisPool)

        self.__mongo_uri = 'mongodb://' + self.__mongo_user + ':' + self.__mongo_pwd + '@' + self.__mongo_host + ':' + self.__mongo_port + '/' + 'weixin'
        self.client = MongoClient(self.__mongo_uri)
        #self.client = MongoClient()

    def getComponentAccessToken(self,appid,appSecret):
        url = 'https://api.weixin.qq.com/cgi-bin/component/api_component_token'
        client = self.client
        db = client.weixin
        try:
            component_verify_ticket = self.r.get(appid + '_'+'component_verify_ticket')
            if component_verify_ticket:
                component_access_token = self.r.get(appid + '_' + 'component_access_token')
                if component_access_token is None:
                    postData = {'component_appid':appid,'component_appsecret':appSecret,'component_verify_ticket':component_verify_ticket.decode('utf-8')}
                    r = self.postRequest(url,json.dumps(postData))
                    res = r.json()
                    if res.get('errcode') is not None:
                        raise Exception(res.get('errmsg'))
                    component_access_token = res.get('component_access_token')
                    expires_in = res.get('expires_in')
                    self.r.set(appid + '_' + 'component_access_token',component_access_token,expires_in)
                return component_access_token
            else:
                raise Exception('Component Verify Ticket is missing!')

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': '','createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            self.logger.error(msg)

        finally:
            client.close()

    def getPreAuthCode(self,appid,appSecret):
        client = self.client
        db = client.weixin

        try:
            pre_auth_code = self.r.get(appid + '_' + 'pre_auth_code')
            if pre_auth_code is None:
                component_access_token = self.getComponentAccessToken(appid,appSecret)
                if component_access_token:
                    url = 'https://api.weixin.qq.com/cgi-bin/component/api_create_preauthcode?component_access_token={}'.format(component_access_token)
                    postData = {'component_appid': appid}

                    r = self.postRequest(url, json.dumps(postData))
                    res = r.json()
                    if res.get('errcode') is not None:
                        raise Exception(res.get('errmsg'))
                    pre_auth_code = res.get('pre_auth_code')
                    expires_in = res.get('expires_in')
                    self.r.set(appid + '_' + 'pre_auth_code', pre_auth_code, expires_in)
                else:
                    raise Exception('Component Access Token is missing!')
            return pre_auth_code

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': '','createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            self.logger.error(msg)

        finally:
            client.close()

    def getAccessTokenFromController(self,appid,appkey):
        try:
            url_ackey = 'http://api.woaap.com/api/ackey'
            paramsDict = {'appid':appid,'appkey':appkey}

            client = self.client
            db = client.weixin

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
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': '','createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            self.logger.error(msg)

        finally:
            client.close()

    def getUserCumulate(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getusercumulate?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getUserCimulate API for account {} from {} to {}'.format(account_name,begin_date,end_date))
            client = self.client
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
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': account_name+','+begin_date+','+end_date,'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
        finally:
            client.close()

    def getArticleTotal(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getarticletotal?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getArticleTotal API for account {} from {} to {}'.format(account_name, begin_date, end_date))
            client = self.client
            db = client.weixin
            postTable = db.weixin_post
            r = self.postRequest(url, postData)
            res = r.json()
            if res.get('errcode'):
                raise Exception(res.get('errmsg'))
            for post in res['list']:
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                post['account_name'] = account_name
                result = postTable.update({'msgid': post['msgid'], 'ref_date': post['ref_date']},
                                          {'$set': post, '$setOnInsert': {
                                              'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                          upsert=True)

            return

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': account_name+','+begin_date+','+end_date,'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            self.logger.error(msg)

        finally:
            client.close()

    def getUpstreamMsg(self, access_token, begin_date, end_date, account_name):
        url = 'https://api.weixin.qq.com/datacube/getupstreammsg?access_token={}'.format(access_token)
        data = {'begin_date': begin_date, 'end_date': end_date}
        postData = json.dumps(data)
        try:
            self.logger.info('Calling getUpstreamMsg API for account {} from {} to {}'.format(account_name, begin_date, end_date))
            client = self.client
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
            db.weixin_error_log.insert({'className': class_name, 'functionName': function_name, 'params': account_name+','+begin_date+','+end_date,'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'msg': msg})
            self.logger.error(msg)
        finally:
            client.close()

    def __str__(self):
        return 'Social API of Weixin'
