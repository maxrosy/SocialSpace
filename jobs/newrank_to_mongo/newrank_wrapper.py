from SocialAPI.SocialAPI.NewRankAPI import NewRankAPI
import argparse

def main():
    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.description = 'Command line interface of test argparser'
    main_parser.add_argument('--from', required=True, help='start time(included), format: YYYY-mm-dd HH:MM:SS')
    main_parser.add_argument('--to', required=True, help='end time(not included), format: YYYY-mm-dd HH:MM:SS')
    main_parser.add_argument('-ia', '--includeAllWords')
    main_parser.add_argument('-in', '--includeAnyWords')
    main_parser.add_argument('-e', '--excludeWords')
    main_parser.add_argument('-p', '--pagelimit', default=5, type=int)
    main_parser.add_argument('-s', '--size', default=5, type=int)
    main_parser.add_argument("--others",
                             help="Any optional argument not mentioned above, example: --others=key1||value1||key2||value2")

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help='Choose one application')

    weixin_parser = subparsers.add_parser('weixin', help='weixin', parents=[main_parser])
    weixin_subparser = weixin_parser.add_subparsers(help='Choose one function')
    weixin_search_subparser = weixin_subparser.add_parser('search_content', parents=[main_parser])

    weixin_article_subparser = weixin_subparser.add_parser('article_content', parents=[main_parser])
    weixin_article_subparser.add_argument('account')
    args = parser.parse_args()
    opt = vars(args)

    if opt.get('others'):
        others_str = opt.get('others').split('||')
        others_dict= dict(zip(others_str[0::2],others_str[1::2]))
        del opt['others']
        opt = {**opt,**others_dict}

    # Remove params whose value are None
    none_key = [key for key in opt.keys() if opt[key] is None]
    for key in none_key:
        del opt[key]
    from_time = opt.pop('from')
    to_time = opt.pop('to')
    newrank = NewRankAPI()
    if opt.get('app') == 'weixin' and opt.get('function') == 'data_combine_search_content':
        newrank.get_weixin_data_combine_search_content(from_time,to_time,**opt)
    elif opt.get('app') == 'weixin' and opt.get('function') == 'account_article_content':
        newrank.get_weixin_account_article_content(account,from_time,to_time,**opt)

if __name__ == "__main__":
    main()