import pandas as pd
import requests
import json
import sys
from datetime import datetime
import configparser
from openpyxl import load_workbook, Workbook
from sqlalchemy import create_engine
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
		#self.__db = self.cfp.get('db','db')
		#self.__table = self.cfp.get('db','table')
		
	def postRequest(self,url, postData):
		
		r = requests.post(url, data=postData)
		return r
		
	def getRequest(self,url,paramsDict):
		
		r = request.get(url, params=paramsDict)
		return r
		
	def cleanRecords(self,df,dedupColumns=[]):
		try:
			df=df.drop_duplicates()
			df=df[df['Platform'] != 'TOTAL']
			#df.info()
			
			df.rename(columns={'Likes/Followers/Visits/Downloads':'Likes'}, inplace = True)
			df['Date Sampled'] = df['Date Sampled'].str.slice(0,10)
			df['Date Sampled'] = pd.to_datetime(df['Date Sampled'],yearfirst=True)
			if dedupColumns != []:
				isDuplicated = df.duplicated(dedupColumns)
				df = df[~isDuplicated]
			df.reset_index(drop=True,inplace=True)
			self.logger.info('Totally %d records remained after ETL' %(len(df)))
			return df
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def writeDataFrameToCsv(self,df,wbname,sheetname):
		try:
			try:
				writer = pd.ExcelWriter(wbname)
				wb = load_workbook(wbname)
					
			except FileNotFoundError:
				self.logger.warn("File %s is not found! Creating one" %(wbname))
				wb = Workbook()
				
			writer.book = wb
			writer.sheets = dict((ws.title, ws) for ws in wb.worksheets)  
					
			df.to_excel(writer,sheetname,index=False)
			writer.save()
		
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def readCsvToDataFrame(filePath,sep=','):	
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
					self.logger.info("Iteration is stopped. Totally, %d loops" %(n))
			df = pd.concat(chunks, ignore_index=True)
			self.logger.info("Totally %d records imported" %(len(df)))
			#df.info()
			return df
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
			
	def syncToDB(self,df,db,table):
		try:
			df['created_time'] = datetime.now()
			df['updated_time'] = datetime.now()
			dblink = 'mysql+mysqldb://{}:{}@{}/{}?charset=utf8'.format(self.__username,self.__password,self.__host,db)
			engine = create_engine(dblink,encoding='utf-8')
			#df.to_sql(table,engine,chunksize=1000,dtype={"Agency": String(50),"Platform":String(50),"Likes":Integer},index=False,if_exists='append',encoding='utf-8')
			df.to_sql(table,engine,chunksize=1000,index=False,if_exists='append')
			
			"""
			# To upsert records to mysql if needed
			conn=MySQLdb.connect(host='127.0.0.1',port=3306,user='root',passwd='Laconia1987',db='pandas',charset='utf8')
			cursor=conn.cursor()
			cursor.execute("insert into social(tagID,Url) values(1,'http://test.com') on duplicate update Url='http://test.com'")
			conn.commit()
			cursor.close()
			conn.close()
			"""
			
		except Exception as e:
			self.logger.error('On line %d - %s' %(sys.exc_info()[2].tb_lineno,e))
			exit(1)
		
	def __str__(self):
		return "Basic API of Social"
		