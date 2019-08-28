import argparse
import sys
sys.path.append('/home/panther/SocialSpace/')
import znanalysis.Spider.HupuAPISail as hupu


def main():

    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.description = 'Command line interface of hupu argparser'
    main_parser.add_argument('-kw', '--keyword', help='search keyword')
    main_parser.add_argument('-p', '--pagelimit', help='max number of pages to search')

    args = main_parser.parse_args()
    opt = vars(args)

    h = hupu.HupuMongo()
    h.get_hupu_data(opt.get('keyword'), opt.get('pagelimit'))


if __name__ == "__main__":
    main()