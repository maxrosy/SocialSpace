import argparse
import sys
sys.path.append('/home/panther/SocialSpace/')
import znanalysis.Spider.DouyinAppApiSail as douyin


def main():

    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.description = 'Command line interface of douyin argparser'
    main_parser.add_argument('-u', '--user_id', help='douyin userid (should be all numbers)')

    args = main_parser.parse_args()
    opt = vars(args)

    d = douyin.DouyinMongo()
    d.get_douyin_data(opt.get('user_id'))


if __name__ == "__main__":
    main()