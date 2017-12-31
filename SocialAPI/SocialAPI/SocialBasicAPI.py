import pandas as pd
import requests
import json
import sys
import boto3
import botocore
from datetime import datetime
import configparser
from openpyxl import load_workbook, Workbook
from sqlalchemy import create_engine, MetaData,Table
from sqlalchemy.types import *
from SocialAPI.Logger.BasicLogger import Logger



class SocialBasicAPI(object):
	
	def __init__(self):
		self.cfp = configparser.ConfigParser()
		self.cfp.read('./conf/social.conf')
		self.logger = Logger('./conf/logging.conf','simpleExample').createLogger()
		self.__username = self.cfp.get('db','user')
		self.__password = self.cfp.get('db','password')
		self.__host = self.cfp.get('db','host')
		self.__port = int(self.cfp.get('db','port'))
		#self.__db = self.cfp.get('db','db')
		#self.__table = self.cfp.get('db','table')
		
	def postRequest(self,url, postData):
		
		r = requests.post(url, data=postData)
		return r
		
	def getRequest(self,url,paramsDict):
		
		r = requests.get(url, params=paramsDict)
		return r
		
	def cleanRecords(self,df,dedupColumns=[]):
		self.logger.info("Calling cleanRecords function")
		try:
			df.drop_duplicates(inplace=True)
			df.fillna('null',inplace=True)
			#df=df[df['Platform'] != 'TOTAL']
			#df.info()
			
			#df.rename(columns={'Likes/Followers/Visits/Downloads':'Likes'}, inplace = True)
			#df['Date Sampled'] = df['Date Sampled'].str.slice(0,10)
			#df['Date Sampled'] = pd.to_datetime(df['Date Sampled'],yearfirst=True)
			beforeETL = len(df)
			if dedupColumns != []:
				isDuplicated = df.duplicated(dedupColumns)
				df = df[~isDuplicated]
			df.reset_index(drop=True,inplace=True)
			afterETL = len(df)
			self.logger.info('Totally {} our of {} records remained after ETL'.format(afterETL, beforeETL))
			return df
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def writeDataFrameToCsv(self,df,filename,sep=','):
		self.logger.info("Calling writeDataFrameToCsv function")
		try:
			df.to_csv(filename,sep=sep,header=True,index=False)
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)	
			
			
	def writeDataFrameToExcel(self,df,wbname,sheetname):
		self.logger.info("Calling writeDataFrameToExcel function")
		try:
			try:
				writer = pd.ExcelWriter(wbname)
				wb = load_workbook(wbname)
					
			except FileNotFoundError:
				self.logger.warn("File {} is not found! Creating one".format(wbname))
				wb = Workbook()
				
			writer.book = wb
			writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)  
					
			df.to_excel(writer,sheetname,index=False)
			writer.save()
		
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def readCsvToDataFrame(filePath,sep=','):	
		self.logger.info("Calling readCsvToDataFrame function")
		try:
			reader = pd.read_csv(filePath,sep=sep,iterator=True)
			loop = True
			chunkSize = 1000
			chunks = []
			n=0
			while loop:
				try:
					chunk = reader.get_chunk(chunkSize)
					chunks.append(chunk)
					n+=1
				except StopIteration:
					loop = False
					self.logger.info("Iteration is stopped. Totally, {} loops".format(n))
			df = pd.concat(chunks, ignore_index=True)
			self.logger.info("Totally {} records imported".format(len(df)))
			#df.info()
			return df
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def syncToDB(self,df,db,table,syncType='append'):
		self.logger.info("Calling syncToDB function")
		try:
			df['created_time'] = datetime.now()
			df['updated_time'] = datetime.now()
			dblink = 'mysql+mysqldb://{}:{}@{}/{}?charset=utf8'.format(self.__username,self.__password,self.__host,db)
			engine = create_engine(dblink,encoding='utf-8')
			#df.to_sql(table,engine,chunksize=1000,dtype={"Agency": String(50),"Platform":String(50),"Likes":Integer},index=False,if_exists='append',encoding='utf-8')
			df.to_sql(table,engine,chunksize=1000,index=False,if_exists=syncType)
			
			"""
			# To upsert records to mysql if needed
			conn=self.connectToDB(db)
			cursor=conn.cursor()
			cursor.execute("insert into social(tagID,Url) values(1,'http://test.com') on duplicate update Url='http://test.com'")
			conn.commit()
			cursor.close()
			conn.close()
			"""
			
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
	
	def insertToDB(self,dbName,tableName,records,type='dataframe'):
		"""
		type can be dict or dataframe, default is dataframe
		"""
		try:
			engine, meta = self.connectToDB(dbName)
			conn = engine.connect()
					
			table = Table(tableName,meta)
			stmt = table.insert()
			if type == 'dataframe':
				res = conn.execute(stmt,records.to_dict('records'))
			elif type == 'dict':
				res = conn.execute(stmt,records)
			else:
				raise Exception("Record Type {} is wrong".format(type))
				
			self.logger.info('{} record(s) have been inserted into {}'.format(res.rowcount,tableName))
			res.close()
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
		finally:
			conn.close()
		
	def connectToDB(self,db):
		
		try:
			dblink = 'mysql+mysqldb://{}:{}@{}/{}?charset=utf8'.format(self.__username,self.__password,self.__host,db)
			engine = create_engine(dblink,encoding='utf-8',echo=False)
			meta = MetaData(bind=engine,reflect=True)
			
			return (engine, meta)
		
		except Exception as e:
			self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
			exit(1)
		
		#conn=MySQLdb.connect(host=self.__host,port=self.__port,user=self.__username,passwd=self.__password,db=db,charset='utf8')
		#return conn
	
	def readFromS3(self,bucketName,remoteFile,localFile):
		self.logger.info("Calling readFromS3 function")
		try:
			s3 = boto3.resource('s3')
			s3.Bucket(bucketName).download_file(remoteFile, localFile)
		
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == "404":
				self.logger.error("The object does not exist.")
			else:
				raise
		
		except Exception as e:
				self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
				exit(1)
				
	def writeToS3(self,bucketName,localFile,remoteFile):
		self.logger.info("Calling writeToS3 function")
		try:
			s3 = boto3.resource('s3')
			data = open(localFile, 'rb')
			s3.Bucket(bucketName).put_object(Key=remoteFile, Body=data)
			
		except Exception as e:
				self.logger.error('On line {} - {}'.format(sys.exc_info()[2].tb_lineno,e))
				exit(1)
				
	def __str__(self):
		return "Basic API of Social"
		
