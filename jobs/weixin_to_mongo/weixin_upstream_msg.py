from SocialAPI.SocialAPI.WeixinAPI import SocialWeixinAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import WeixinAccount
import datetime

if __name__ == '__main__':
    rootPath = Helper().getRootPath()

    weixin = SocialWeixinAPI()

    session = weixin.createSession()
    accountInfo = session.query(WeixinAccount.appid, WeixinAccount.appkey, WeixinAccount.account_name,WeixinAccount.new_url) \
        .all()

    #first_date = datetime.datetime.strptime("2018-11-07", "%Y-%m-%d")
    #last_date = datetime.datetime.strptime("2018-11-07", "%Y-%m-%d")
    first_date = datetime.date.today() + datetime.timedelta(-1)
    last_date = datetime.date.today() + datetime.timedelta(-1)

    for account in accountInfo:
        run_date = first_date
        while run_date <= last_date:
            begin_date = run_date.strftime("%Y-%m-%d")
            end_date = begin_date
            if account[3] == 1:
                access_ackey_or_token = weixin.getAckey(account[0], account[1], account[3])
            else:
                access_ackey_or_token = weixin.getAccessTokenFromController(account[0], account[1])
            res = weixin.getUpstreamMsg(access_ackey_or_token, begin_date, end_date, account[2], account[3])
            run_date = run_date + datetime.timedelta(days=1)

    session.close()