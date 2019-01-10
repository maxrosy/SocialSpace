from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import IdataAccount
import datetime


if __name__ == '__main__':

    idata = IdataAPI()

    session = idata.createSession()
    accountInfo = session.query(IdataAccount.appCode, IdataAccount.type).filter(IdataAccount.status == 1).all()
    accountInfo = [('toutiao','kwindex')]

    createBeginDate = (datetime.date.today()+datetime.timedelta(-1)).strftime("%Y-%m-%d")
    createEndDate = createBeginDate
    #createBeginDate = '2019-01-09'
    #createEndDate = '2019-01-09'
    for account in accountInfo:
        idata.getZanDouData(createBeginDate,createEndDate,size=50,appCode=account[0],type=account[1])


    session.close()