from types import MethodDescriptorType
from kafka import KafkaProducer
import json
import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent

BROKERS = ["localhost:9092"]
TOPIC_NAME = "metacritic_meta_topic"

producer = KafkaProducer(bootstrap_servers = BROKERS)

def get_lastpage(res) :
    lastpage = int(res.xpath('//*[@class="page last_page"]/a/text()').extract()[0])
    return lastpage

def get_link(res) :
    links = res.xpath(
            '//*[@id="main_content"]/div[1]/div[2]/div/div[1]/div/div/table/tr/td[2]/a/@href'
        ).extract()
    return links


def get_urllist():
    BASE_URL = 'https://www.metacritic.com/'
    GAME_URL = 'browse/games'
    DATE_URL = '/release-date/available'
    SORT_URL = '/date?view=condensed'
    PAGE_URL = '&page='
    PAGE_NUM = '0'

    platformlist = ['/pc', '/switch', '/ios', '/ps4', '/ps5']
    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}



    urllist = []
    lastpage = 0

    for platform in platformlist:
        print(platform)
        try:
            url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + PAGE_NUM
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')
            lastpage = get_lastpage(res)
        except:
            pass

        for page in range(lastpage):

            try:
                url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + str(page)
                req = requests.get(url, headers=header)
                res = TextResponse(req.url, body=req.text, encoding='utf-8')
                link = get_link(res)
                urllist += link
                
            except:
                pass

    return urllist


def get_criticlist(res, scorelist) :
    criticlist = []
    for i in range(1,len(scorelist)+20):
        a=''
        try :
            a = res.xpath(f'//*[@class="reviews critic_reviews"]/li[{i}]//*[@class="review_critic"]/div/a/text()').extract()[0]
        except :
            a = ''
        if len(a)<1 :
            try :
                a = res.xpath(f'//*[@class="reviews critic_reviews"]/li[{i}]//*[@class="review_critic"]/div/text()').extract()[0]
            except :
                pass
        if a!='' :
            criticlist.append(a)
    return criticlist

def get_scorelist(res) :
    scorelist = res.xpath(
        '//*[@id="main"]/div[5]/div/ol/li/div/div/div/div/div/div[1]/div[1]/div[2]/div/text()'
    ).extract()
    return scorelist


def get_contentlist(res, scorelist):
    contentlist = []
    for i in range(1, len(scorelist) + 20):
        temp_contentlist = ''
        temp_content = ''
        try:
            temp_contentlist = res.xpath(
                f'//*[@class="body product_reviews"]/ol/li[{i}]//*[@class="review_body"]/text()'
            ).extract()
            temp_content = ' '.join(temp_contentlist).replace('  ', '').replace('\n', '').replace('\r', '')
        except:
            temp_content = ''
        if len(temp_content) == 0:
            pass
        else:
            contentlist.append(temp_content)

    if len(contentlist) < len(scorelist):
        contentlist = []
        temp_contentlist = res.xpath(
            '//*[@class="body product_reviews"]/ol/li//*[@class="review_body"]/text()').extract()
        for temp in temp_contentlist:
            temp = temp.strip()
            contentlist.append(temp)

            

    return contentlist



def get_gamename(res) :
    gamename = res.xpath('//*[@id="main"]/div[1]/div[2]/a/h1/text()').extract()[0]
    return gamename


def get_platform(tempurl) :
    try :
        platform = tempurl.split('/')[2]
    except :
        platform = ''
    return platform


def get_meta(urllist):
    BASE_URL = 'https://www.metacritic.com'

    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}



    for tempurl in urllist:
        url = BASE_URL + tempurl + '/critic-reviews'
        platform = get_platform(tempurl)

        try:
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')
            gamename = get_gamename(res)
            scorelist = get_scorelist(res)
            contentlist = get_contentlist(res, scorelist)
            criticlist = get_criticlist(res, scorelist)
            

            for i in range(len(criticlist)):
                if len(scorelist) == 0:
                    DOC = {"title":gamename, "platform":platform, "criticname":0, "criticscore":0, "criticcontent":0}
                else:
                    DOC = {"title":gamename, "platform":platform, "criticname":criticlist[i], "criticscore":scorelist[i], "criticcontent":contentlist[i]}

                producer.send(TOPIC_NAME, json.dumps(DOC).encode("utf-8"))
                print(DOC)
                producer.flush()
        except:
            print('ERROR!!!!!!!!' + tempurl)


def metacrawl_meta():
    urllist = get_urllist()
    get_meta(urllist)

metacrawl_meta()

    