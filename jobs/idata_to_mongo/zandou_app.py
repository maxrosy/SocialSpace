from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import IdataAccount
import datetime
import time


if __name__ == '__main__':


    idata = IdataAPI()

    session = idata.createSession()
    accountInfo = session.query(IdataAccount.appCode, IdataAccount.type).filter(IdataAccount.status == 1).all()
    #accountInfo = [('hupu','comment')]

    createBeginDate = (datetime.date.today()+ datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    #createEndDate = createBeginDate
    createEndDate = datetime.date.today().strftime("%Y-%m-%d")
    #createBeginDate = '2019-01-09'
    #createEndDate = '2019-01-09'
    for account in accountInfo:
        idata.getZanDouData(createBeginDate,createEndDate,size=100,appCode=account[0],type=account[1])
        time.sleep(0.5)


    session.close()
