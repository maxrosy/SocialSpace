from SocialAPI.SocialAPI.WeixinAPI import SocialWeixinAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop
import sys
import getopt
from SocialAPI.Model import WeixinAccount

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weixin = SocialWeixinAPI()

    session = weixin.createSession()
    accountInfo = session.query(WeixinAccount.appid,WeixinAccount.appkey,WeixinAccount.account_name).all()
    begin_date = weixin.getStrTime(-1).split(' ')[0]
    end_date = weixin.getStrTime(-1).split(' ')[0]

    for account in accountInfo:
        for n in range(1):
            begin_date = weixin.getStrTime(-(n+1)).split(' ')[0]
            end_date = weixin.getStrTime(-(n+1)).split(' ')[0]
            accessToken = weixin.getAccessTokenFromController(account[0],account[1])
            res = weixin.getUserCumulate(accessToken,begin_date,end_date,account[2])

    session.close()