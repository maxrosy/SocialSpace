import pandas as pd
from SocialAPI.SocialAPI.SocialWeiboAPI import SocialWeiboAPI
from SocialAPI.Helper import Helper
import asyncio
import uvloop

if __name__ == '__main__':
    rootPath = Helper().getRootPath()
    df = pd.read_csv(rootPath + '/input/uid.csv',';')
    uidList = list(df['uid'].apply(str))

    n = 103 # len(df)
    uidGroup = [','.join(uidList[i:i+100]) for i in range(0,n,100)]
    #uidGroup = ['1609648201,5934019851,1768660152']
    weibo = SocialWeiboAPI()


    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    tasks = [asyncio.ensure_future(weibo.getUsersCountBatch(uids),loop=loop) for uids in uidGroup]
    loop.run_until_complete(asyncio.wait(tasks))
    result = [task.result() for task in tasks]
    df = pd.concat(result, ignore_index=True)
    filePath = rootPath + '/output/weibo_user_growth_daily'
    os.makedirs(filePath, exist_ok=True)
    fileName = '/weibo_user_growth_daily_' + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + '.csv'
    weibo.writeDataFrameToCsv(df, filePath + fileName, sep="|")
    loop.close()
