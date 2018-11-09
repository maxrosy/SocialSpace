from SocialAPI.SocialAPI.WeixinAPI import SocialWeixinAPI
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
import os
from SocialAPI.Helper import Helper
from SocialAPI.Model import Kol
import datetime,time
import pandas as pd
from pymongo import MongoClient


if __name__ == '__main__':
	"""
	weibo = SocialWeiboAPI()
	#weibo.getStatusRepostTimeline('4257051804582356')

	client = MongoClient()
	db = client.weibo
	crawlTable = db.weibo_post_crawl

	posts = crawlTable.find()

	for post in posts:
		if isinstance(post['createdTime'],int):
			post['createdTime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(post['createdTime']))
		if isinstance(post['updatedTime'],int):
			post['updatedTime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(post['updatedTime']))
		crawlTable.update({'_id':post['_id']},{'$set':post})

	client.close()
	"""

	weixin = SocialWeixinAPI()
	access_token = weixin.getComponentAccessToken()
