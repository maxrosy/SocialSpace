import re
import rsa
import time
import json
import base64
import binascii
import requests
import urllib.parse
from SocialAPI.Logger.BasicLogger import Logger
from SocialAPI.Helper import Helper

class WeiBoCrawler(object):
    """
    First login then crawl as a user
    """

    def __init__(self):
        """
        constructor
        """
        self.user_name = None
        self.pass_word = None
        self.user_uniqueid = None
        self.user_nick = None

        self.__rootPath = Helper().getRootPath()
        #self.logger = Logger(self.__rootPath + '/conf/logging.conf', 'simpleExample').createLogger()
        self.logger_access = Logger(self.__rootPath + '/conf/logging.conf', 'logger_access').createLogger()
        self.logger_error = Logger(self.__rootPath + '/conf/logging.conf', 'logger_error').createLogger()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0"})
        self.session.get("http://weibo.com/login.php")
        return

    def login(self, user_name, pass_word):
        """
        login weibo.com, return True or False
        """
        self.user_name = user_name
        self.pass_word = pass_word
        self.user_uniqueid = None
        self.user_nick = None
        self.logger_access.info('Login in process for {}'.format(user_name))
        # get json data
        s_user_name = self.get_username()
        json_data = self.get_json_data(su_value=s_user_name)
        if not json_data:
            return False
        s_pass_word = self.get_password(json_data["servertime"], json_data["nonce"], json_data["pubkey"])

        # make post_data
        post_data = {
            "entry": "weibo",
            "gateway": "1",
            "from": "",
            "savestate": "7",
            "userticket": "1",
            "vsnf": "1",
            "service": "miniblog",
            "encoding": "UTF-8",
            "pwencode": "rsa2",
            "sr": "1280*800",
            "prelt": "529",
            "url": "http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack",
            "rsakv": json_data["rsakv"],
            "servertime": json_data["servertime"],
            "nonce": json_data["nonce"],
            "su": s_user_name,
            "sp": s_pass_word,
            "returntype": "TEXT",
        }

        # get captcha code
        if json_data["showpin"] == 1:
            url = "http://login.sina.com.cn/cgi/pin.php?r=%d&s=0&p=%s" % (int(time.time()), json_data["pcid"])
            with open("/home/panther/Downloads/captcha.jpeg", "wb") as file_out:
                file_out.write(self.session.get(url).content)
            file_out.close()
            code = input("请输入验证码:")
            post_data["pcid"] = json_data["pcid"]
            post_data["door"] = code

        # login weibo.com
        login_url_1 = "http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)&_=%d" % int(time.time())
        json_data_1 = self.session.post(login_url_1, data=post_data).json()
        if json_data_1["retcode"] == "0":
            params = {
                "callback": "sinaSSOController.callbackLoginStatus",
                "client": "ssologin.js(v1.4.18)",
                "ticket": json_data_1["ticket"],
                "ssosavestate": int(time.time()),
                "_": int(time.time()*1000),
            }
            response = self.session.get("https://passport.weibo.com/wbsso/login", params=params)
            json_data_2 = json.loads(re.search(r"\((?P<result>.*)\)", response.text).group("result"))
            if json_data_2["result"] is True:
                self.user_uniqueid = json_data_2["userinfo"]["uniqueid"]
                self.user_nick = json_data_2["userinfo"]["displayname"]
                self.logger_access.info("WeiboLogin succeeded: {}".format(json_data_2))
            else:
                self.logger_error.error("WeiboLogin failed: {}".format(json_data_2))

        else:
            self.logger_error.error("WeiboLogin failed: {}".format(json_data_1))
        return True if self.user_uniqueid and self.user_nick else False

    def get_username(self):
        """
        get legal username
        """
        username_quote = urllib.parse.quote_plus(self.user_name)
        username_base64 = base64.b64encode(username_quote.encode("utf-8"))
        return username_base64.decode("utf-8")

    def get_json_data(self, su_value):
        """
        get the value of "servertime", "nonce", "pubkey", "rsakv" and "showpin", etc
        """
        params = {
            "entry": "weibo",
            "callback": "sinaSSOController.preloginCallBack",
            "rsakt": "mod",
            "checkpin": "1",
            "client": "ssologin.js(v1.4.18)",
            "su": su_value,
            "_": int(time.time()*1000),
        }
        try:
            response = self.session.get("http://login.sina.com.cn/sso/prelogin.php", params=params)
            json_data = json.loads(re.search(r"\((?P<data>.*)\)", response.text).group("data"))
        except Exception as e:
            json_data = {}
            self.logger_error.error("WeiboLogin get_json_data error: {}".format(e))
        self.logger_access.debug("Weibologin get_json_data: {}".format(json_data))
        return json_data

    def get_password(self, servertime, nonce, pubkey):
        """
        get legal password
        """
        string = (str(servertime) + "\t" + str(nonce) + "\n" + str(self.pass_word)).encode("utf-8")
        public_key = rsa.PublicKey(int(pubkey, 16), int("10001", 16))
        password = rsa.encrypt(string, public_key)
        password = binascii.b2a_hex(password)
        return password.decode()


    def getImpressions(self,html):
        matchObj = re.search(u'阅读(\d*)', html)
        if matchObj:
            return matchObj.group(1)
        else:
            return 0


    def getForwards(self,html):
        html = html.replace('\r\n', '')
        matchObj = re.search(r'WB_feed_handle(.*?)ficon_forward(.*?)<em>(\d+)<\\/em>', html)
        if matchObj:
            return matchObj.group(3)
        else:
            return 0


    def getComments(self,html):
        html = html.replace('\r\n', '')
        matchObj = re.search(r'WB_feed_handle(.*?)ficon_repeat(.*?)<em>(\d+)<\\/em>', html)
        if matchObj:
            return matchObj.group(3)
        else:
            return 0


    def getLikes(self,html):
        html = html.replace('\r\n','')
        matchObj = re.search(r'WB_feed_handle(.*?)ficon_praised(.*?)<em>(\d+)<\\/em>', html)
        if matchObj:
            return matchObj.group(3)
        else:
            return 0

    def crawlPage(self,url):
        response = self.session.get(url)
        if response.status_code == 200:
            self.logger_access.info("Crawl page {} succeeded".format(url))
            return response.text
        else:
            self.logger_error.error("Crawl page failed - ".format(response.reason))

