import sys
sys.path.append('home/panther/SocialSpace')
from SocialAPI.SocialAPI.WeiboAPI import SocialWeiboAPI
import argparse


def main():
    choices = ['statuses_show_batch_biz','get_comment_by_since_id','get_attitude_by_since_id','get_repost_by_since_id']
    try:
        parser = argparse.ArgumentParser()
        parser.description = 'Command line interface of Weibo'
        parser.add_argument("-f", "--function", required=True, choices=choices,help="Choose one function")
        parser.add_argument("-m","--mid",help="Weibo post ID")
        parser.add_argument("-i","--ids",help="Weibo post IDs")
        parser.add_argument("--others",
                            help="Any optional argument not mentioned above, example: --others=key1//value1//key2//value2")
        args = parser.parse_args()
        opt = vars(args)

        if opt.get('others'):
            others_str = opt.get('others').split('//')
            others_dict = dict(zip(others_str[0::2], others_str[1::2]))
            del opt['others']
            opt = {**opt, **others_dict}

        # Remove params whose value are None
        none_key = [key for key in opt.keys() if opt[key] is None]
        for key in none_key:
            del opt[key]
        func=opt.pop('function')

        weibo = SocialWeiboAPI()
        getattr(weibo, func)(**opt)


    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()