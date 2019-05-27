from SocialAPI.SocialAPI.IdataAPI import IdataAPI
from SocialAPI.Helper import Helper
from SocialAPI.Model import IdataAccount
import datetime
import threading


if __name__ == '__main__':


    idata = IdataAPI()

    session = idata.createSession()
    accountInfo = session.query(IdataAccount.appCode, IdataAccount.type).filter(IdataAccount.status == 1).all()
    #accountInfo = [('zhihu','comment')]
    session.close()

    createBeginDate = (datetime.date.today()+ datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    #createEndDate = createBeginDate
    createEndDate = datetime.date.today().strftime("%Y-%m-%d")
    #createBeginDate = '2019-01-09'
    #createEndDate = '2019-01-09'
    """
    for account in accountInfo:
        idata.getZanDouData(createBeginDate,createEndDate,size=100,appCode=account[0],type=account[1])
    """
    threads = []

    for account in accountInfo:
        t = threading.Thread(target=idata.get_zandou_data, args=(createBeginDate,createEndDate,), kwargs={'size':1000,'appCode':account[0],'type':account[1]})
        threads.append(t)

    for t in threads:
        t.start()
        while True:
            if len(threading.enumerate()) <5:
                break

