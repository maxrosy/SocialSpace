from SocialAPI.SocialAPI.IdataAPI import IdataAPI
import argparse
from kafka import KafkaProducer

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.description='Command line interface of idata'
        parser.add_argument("post_type", help="The name of the post type")
        parser.add_argument("app", help="The name of the application")
        parser.add_argument("-u", "--uid", help="User ID")
        parser.add_argument("-k","--kw", help="Search keyword")
        parser.add_argument("-c","--catid", help="Category ID")
        parser.add_argument("-t","--tagid", help="Tag ID")
        parser.add_argument("-i","--id",help="Can be unique id of item like post id")
        parser.add_argument("-p", '--pagelimit', help="The number of page to be fetched",type=int)
        parser.add_argument("--others",help="Any optional argument not mentioned above, example: --others=key1//value1//key2//value2")
        args = parser.parse_args()
        opt = vars(args)

        if opt.get('others'):
            others_str = opt.get('others').split('//')
            others_dict= dict(zip(others_str[0::2],others_str[1::2]))
            del opt['others']
            opt = {**opt,**others_dict}

        # Remove params whose value are None
        none_key = [key for key in opt.keys() if opt[key] is None]
        for key in none_key:
            del opt[key]

        idata = IdataAPI()
        if opt.get('app') == 'xiaohongshu' and opt.get('post_type') == 'post' and opt.get('kw'):
            ids = idata.get_post_id(**opt)
            opt.pop('kw')
            for _ in ids:
                opt['id'] = _
                idata.get_idata_data(**opt)
        else:
            idata.get_idata_data(**opt)

        try:
            topic = opt.get('app')+'_'+opt.get('post_type')
            producer = KafkaProducer(bootstrap_servers=['172.16.42.3:9092'])
            producer.send(topic, b'max4', partition=0)
        except Exception as e:
            print(e)
        finally:
            producer.close()


    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()

