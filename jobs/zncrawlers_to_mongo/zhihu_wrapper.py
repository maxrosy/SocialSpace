import argparse
import sys
sys.path.append('/home/panther/SocialSpace/')
import znanalysis.Spider.ZhihuApiSail as zhihu


def main():

    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.description = 'Command line interface of hupu argparser'
    main_parser.add_argument('-kw', '--keyword', help='search keyword')
    main_parser.add_argument('-q', '--question_id', help='question id used to search for answers')
    main_parser.add_argument('-p', '--pagelimit', help='max number of pages to search')
    main_parser.add_argument('-t', '--type', help='can be question or answer')

    args = main_parser.parse_args()
    opt = vars(args)

    z = ZhihuMongo()
    if opt.get('t') == 'question':
        z.get_zhihu_question(opt.get('kw'), opt.get('p'))
    if opt.get('t') == 'answer':
        z.get_zhihu_answers(opt.get('q'), opt.get('p'))


if __name__ == "__main__":
    main()
