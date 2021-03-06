from pymongo import MongoClient
from .SocialBasicAPI import SocialBasicAPI
from SocialAPI.Helper import Helper
import sys, time
from datetime import datetime
import urllib
from urllib.parse import quote
import pandas as pd
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

class IdataAPI(SocialBasicAPI):

    def __init__(self):
        super(IdataAPI, self).__init__()
        self.__apiToken = self.cfp.get('api', 'idata')
        self.__rootPath = Helper().getRootPath()
        self.__mongo_user = urllib.parse.quote_plus(self.cfp.get('mongodb_idata','user'))
        self.__mongo_pwd = urllib.parse.quote_plus(self.cfp.get('mongodb_idata','pwd'))
        self.__mongo_host = self.cfp.get('mongodb','host')
        self.__mongo_port = self.cfp.get('mongodb','port')
        self.__idata_url = self.cfp.get('idata_platform','url_base')

        self.__mongo_uri = 'mongodb://' + self.__mongo_user + ':' + self.__mongo_pwd + '@' + self.__mongo_host + ':' + self.__mongo_port + '/' + 'idata'
        self.client = MongoClient(self.__mongo_uri)

    def get_zandou_data(self, createBeginDate, createEndDate, **kwargs):

        self.logger_access.info("Calling getZanDouData with params {}".format(kwargs))
        client = self.client
        db = client['idata']
        try:

            url = self.__idata_url + '/zandou'

            paramsDict = kwargs.copy()
            paramsDict['apikey'] = self.__apiToken
            paramsDict['createBeginDate'] = createBeginDate
            paramsDict['createEndDate'] = createEndDate


            tableName = paramsDict.get('appCode') + '_' + paramsDict.get('type')
            postTable = db[tableName]

            # Create Indexes if new
            if not postTable.index_information() and paramsDict.get('type') in ('answer','reply','comment'):
                postTable.create_index([('id', 1)])
                postTable.create_index([('publishDate',-1)])
                postTable.create_index([('updatedTime',-1)])
            elif not postTable.index_information() and paramsDict.get('type') not in ('answer','reply','comment'):
                postTable.create_index([('id',1),('ref_date',-1)],unique=True)
                postTable.create_index([('publishDate', -1)])
                postTable.create_index([('updatedTime', -1)])


            loop = True
            total_posts = 0
            while loop:
                try:
                    postList = list()
                    r = self.getRequest(url, paramsDict)
                    res = r.json()

                    # PageToken returned but next page missing
                    if paramsDict.get('pageToken') and res['retcode'] == '100002':
                        raise Exception('Next Page {} not Found! - {}'.format(paramsDict.get('pageToken'),tableName))
                    if res['retcode'] != '000000':
                        raise Exception('{} - {}'.format(res['message'],tableName))

                    # Remove html column, coz it is too long and useless
                    if tableName in ('weixin_post', 'weixinpro_post'):
                        postDataFrame = pd.DataFrame(res['data'])
                        postDataFrame.drop('html',axis=1,inplace=True)
                        postList += postDataFrame.to_dict('records')
                    else:
                        postList += res['data']

                    if not postList:
                        self.logger_access.info(
                            'No post returned between {} and {} for {}'.format(createBeginDate, createEndDate,
                                                                               tableName))
                        return

                    if paramsDict.get('type') in ('answer', 'reply', 'comment'):
                        #print('{} records before dedup - {}'.format(len(postList), tableName))
                        postDataFrame = pd.DataFrame(postList)
                        postDataFrame.drop_duplicates('id', inplace=True)
                        postList = postDataFrame.to_dict('records')
                        #print('{} records after dedup - {}'.format(len(postList), tableName))

                    update_operations = list()
                    for post in postList:
                        if not post.get('id'):
                            self.logger_error.error('ID missing with post{} for {}'.format(post, tableName))
                            continue
                        post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        x = time.localtime(post['createDate'])
                        post['ref_date'] = time.strftime('%Y-%m-%d', x)

                        if paramsDict.get('type') in ('answer', 'reply', 'comment'):
                            op = UpdateOne({'id': post['id']},
                                           {'$set': post, '$setOnInsert': {
                                               'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                           upsert=True)

                        else:
                            op = UpdateOne({'id': post['id'], 'ref_date': post['ref_date']},
                                           {'$set': post, '$setOnInsert': {
                                               'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                           upsert=True)

                        update_operations.append(op)

                    postTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
                    total_posts += len(postList)
                    self.logger_access.info('{} records have been fetched. Totally {} records - {}'.format(total_posts,res['total'],tableName))
                    time.sleep(0.1)

                    # Summarize the ecomm product so as to make it easy to get sku options
                    if paramsDict.get('type') in ('product'):
                        summary_table_name = tableName + '_summary'
                        summary_table = db[summary_table_name]
                        postDataFrame = pd.DataFrame(postList)
                        postDataFrame.drop_duplicates('id', inplace=True)
                        postList = postDataFrame.to_dict('records')
                        update_operations = list()
                        for post in postList:
                            post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            op = UpdateOne({'id': post['id']},
                                           {'$set': post, '$setOnInsert': {
                                               'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                           upsert=True)

                            update_operations.append(op)

                        summary_table.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
                        time.sleep(0.1)


                    if not res['hasNext']:
                        raise StopIteration
                    paramsDict['pageToken'] = res['pageToken']
                    #self.logger_access.info('pageToken is {}'.format(res['pageToken']))

                except StopIteration:
                    loop = False

        except BulkWriteError as e:
            raise Exception(e.details)

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.idata_error_log.insert({'className': class_name, 'functionName': function_name, 'params': kwargs,
                                        'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
            self.logger_error.error(msg)
            exit(1)

    def get_idata_data(self,**kwargs):
        self.logger_access.info("Calling Idata with params {}".format(kwargs))
        client = self.client
        db = client['idata']
        try:

            url = self.__idata_url + '/' + kwargs['post_type'] + '/' + kwargs['app']
            #headers = {"Accept-Encoding": "gzip", "Connection": "close"}

            paramsDict = kwargs.copy()
            paramsDict['apikey'] = self.__apiToken
            tableName = paramsDict.get('app') + '_' + paramsDict.get('post_type')

            del paramsDict['post_type']
            del paramsDict['app']
            paramsDict['size']=20
            page_limit = kwargs.get('pagelimit',5)
            page_num = 1
            loop = True
            total_posts = 0
            retry_num = 0
            retry_max = 10

            while loop and page_num<=page_limit and retry_num<retry_max:
                try:
                    postList = list()
                    r = self.getRequest(url, paramsDict)
                    res = r.json()

                    # PageToken returned but next page missing
                    if paramsDict.get('pageToken') and res['retcode'] == '100002':
                        raise Exception('Next Page {} not Found! - {}'.format(paramsDict.get('pageToken'), tableName))
                    if res['retcode'] != '000000':
                        if res['retcode'] in ['100000','100001']:
                            retry_num += 1
                            continue
                        raise Exception('{} - {}'.format(res['message'], tableName))


                    # Remove html column, coz it is too long and useless
                    if tableName in ('weixin_post', 'weixinpro_post'):
                        postDataFrame = pd.DataFrame(res['data'])
                        postDataFrame.drop('html', axis=1, inplace=True)
                        postList += postDataFrame.to_dict('records')
                    else:
                        postList += res['data']

                    if not postList:
                        self.logger_access.info(
                            'No post returned for {}'.format(tableName))
                        return

                    if paramsDict.get('post_type') in ('answer', 'reply', 'comment'):
                        # print('{} records before dedup - {}'.format(len(postList), tableName))
                        postDataFrame = pd.DataFrame(postList)
                        postDataFrame.drop_duplicates('id', inplace=True)
                        postList = postDataFrame.to_dict('records')
                        # print('{} records after dedup - {}'.format(len(postList), tableName))

                    self.update_mongodb(postList,**kwargs)

                    total_posts += len(postList)
                    self.logger_access.info(
                        '{} records have been fetched. Totally {} records - {}'.format(len(postList),total_posts,tableName))
                    time.sleep(0.1)

                    # Summarize the ecomm product so as to make it easy to get sku options
                    if kwargs.get('post_type','') in ('product'):
                        summary_table_name = tableName + '_summary'
                        summary_table = db[summary_table_name]
                        postDataFrame = pd.DataFrame(postList)
                        postDataFrame.drop_duplicates('id', inplace=True)
                        postList = postDataFrame.to_dict('records')
                        update_operations = list()
                        for post in postList:
                            post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            op = UpdateOne({'id': post['id']},
                                           {'$set': post, '$setOnInsert': {
                                               'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                           upsert=True)

                            update_operations.append(op)

                        summary_table.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
                        time.sleep(0.1)

                    if not res['hasNext']:
                        raise StopIteration
                    paramsDict['pageToken'] = res['pageToken']
                    page_num += 1
                    # self.logger_access.info('pageToken is {}'.format(res['pageToken']))

                except StopIteration:
                    loop = False


        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.idata_error_log.insert({'className': class_name, 'functionName': function_name, 'params': kwargs,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
            self.logger_error.error(msg)
            exit(1)

    def get_post_id(self,**kwargs):
        self.logger_access.info("Calling Idata with params {}".format(kwargs))
        try:
            client = self.client
            db = client['idata']

            url = self.__idata_url + '/' + kwargs['post_type'] + '/' + kwargs['app']
            headers = {"Accept-Encoding": "gzip", "Connection": "close"}

            paramsDict = kwargs.copy()
            paramsDict['apikey'] = self.__apiToken
            tableName = paramsDict.get('app') + '_' + paramsDict.get('post_type')

            del paramsDict['post_type']
            del paramsDict['app']

            page_limit = kwargs.get('pagelimit', 2)
            page_num = 1
            loop = True
            retry_num = 0
            retry_max = 10
            postList = list()
            total_posts = 0
            while loop and page_num <= page_limit and retry_num < retry_max:
                try:

                    r = self.getRequest(url, paramsDict, headers=headers)
                    res = r.json()

                    # PageToken returned but next page missing
                    if paramsDict.get('pageToken') and res['retcode'] == '100002':
                        raise Exception('Next Page {} not Found! - {}'.format(paramsDict.get('pageToken'), tableName))
                    if res['retcode'] != '000000':
                        if res['retcode'] in ['100000', '100001']:
                            retry_num += 1
                            continue
                        raise Exception('{} - {}'.format(res['message'], tableName))
                    postList += res['data']
                    if not postList:
                        self.logger_access.info(
                            'No post returned for {}'.format(tableName))
                        return

                    total_posts += len(postList)
                    self.logger_access.info(
                        '{} records have been fetched. Totally {} records - {}'.format(len(res['data']), res['count'],
                                                                                       tableName))
                    time.sleep(0.1)

                    if not res['hasNext']:
                        raise StopIteration
                    paramsDict['pageToken'] = res['pageToken']
                    page_num += 1
                    # self.logger_access.info('pageToken is {}'.format(res['pageToken']))

                except StopIteration:
                    loop = False
            return [post['id'] for post in postList]

        except Exception as e:
            class_name = self.__class__.__name__
            function_name = sys._getframe().f_code.co_name
            msg = 'On line {} - {}'.format(sys.exc_info()[2].tb_lineno, e)
            db.idata_error_log.insert({'className': class_name, 'functionName': function_name, 'params': kwargs,
                                       'createdTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 'msg': msg})
            self.logger_error.error(msg)
            exit(1)

    def update_mongodb(self,postList,**kwargs):

        try:
            client = self.client
            db = client['idata']
            paramsDict = kwargs.copy()
            tableName = paramsDict.get('app') + '_' + paramsDict.get('post_type')
            postTable = db[tableName]

            del paramsDict['post_type']
            del paramsDict['app']

            # Create Indexes if new
            if not postTable.index_information() and paramsDict.get('type') in ('answer', 'reply', 'comment'):
                postTable.create_index([('id', 1)])
                postTable.create_index([('publishDate', -1)])
                postTable.create_index([('updatedTime', -1)])
            elif not postTable.index_information() and paramsDict.get('type') not in ('answer', 'reply', 'comment'):
                postTable.create_index([('id', 1), ('ref_date', -1)], unique=True)
                postTable.create_index([('publishDate', -1)])
                postTable.create_index([('updatedTime', -1)])

            update_operations = list()
            for post in postList:
                if not post.get('id'):
                    self.logger_error.error('ID missing with post{} for {}'.format(post, tableName))
                    continue
                post['updatedTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # x = time.localtime(post['createDate'])
                # post['ref_date'] = time.strftime('%Y-%m-%d', x)
                post['ref_date'] = datetime.now().strftime('%Y-%m-%d')

                if paramsDict.get('type') in ('answer', 'reply', 'comment'):
                    op = UpdateOne({'id': post['id']},
                                   {'$set': post, '$setOnInsert': {
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                   upsert=True)

                else:
                    op = UpdateOne({'id': post['id'], 'ref_date': post['ref_date']},
                                   {'$set': post, '$setOnInsert': {
                                       'createdTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}},
                                   upsert=True)

                update_operations.append(op)

            result = postTable.bulk_write(update_operations, ordered=False, bypass_document_validation=False)
            return result.upserted_ids

        except BulkWriteError as e:
            raise Exception(e.details)
        except Exception as e:
            self.logger_error.error(e)
            exit(1)

    def __str__(self):
        return "Idata APIs"