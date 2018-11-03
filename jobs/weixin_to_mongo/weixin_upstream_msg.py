from SocialAPI.SocialAPI.WeixinAPI import SocialWeixinAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import WeixinAccount
import datetime

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weixin = SocialWeixinAPI()

    session = weixin.createSession()
    accountInfo = session.query(WeixinAccount.appid,WeixinAccount.appkey,WeixinAccount.account_name).all()

    #first_date = datetime.strptime("2014-01-01", "%Y-%m-%d")
    #last_date = datetime.strptime("2014-12-31", "%Y-%m-%d")
    first_date = datetime.date.today() + datetime.timedelta(-1)
    last_date = datetime.date.today() + datetime.timedelta(-1)

    for account in accountInfo:
        accessToken = None
        run_date = first_date
        while run_date <= last_date:
            begin_date = run_date.strftime("%Y-%m-%d")
            end_date = begin_date
            if accessToken is None:
                accessToken = weixin.getAccessTokenFromController(account[0], account[1])
            res = weixin.getUpstreamMsg(accessToken, begin_date, end_date, account[2])
            run_date = run_date + datetime.timedelta(days=1)

    session.close()