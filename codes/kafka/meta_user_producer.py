from types import MethodDescriptorType
from kafka import KafkaProducer
import json
import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent

BROKERS = ["localhost:9092"]
TOPIC_NAME = "metacritic_user_topic"

producer = KafkaProducer(bootstrap_servers = BROKERS)

def get_lastpage(res) :
    lastpage = int(res.xpath('//*[@class="page last_page"]/a/text()').extract()[0])
    return lastpage

def get_link(res) :
    links = res.xpath(
            '//*[@id="main_content"]/div[1]/div[2]/div/div[1]/div/div/table/tr/td[2]/a/@href'
        ).extract()
    return links


def get_urllist() :
    BASE_URL = 'https://www.metacritic.com/'
    GAME_URL = 'browse/games'
    DATE_URL = '/release-date/available'
    SORT_URL = '/date?view=condensed'
    PAGE_URL = '&page='
    PAGE_NUM = '0'

    platformlist = ['/pc', '/switch', '/ios', '/ps4', '/ps5']
    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}

    # url = BASE_URL+GAME_URL+DATE_URL+PLATFORM_URL+SORT_URL+PAGE_URL
    platformlist = ['/pc', '/switch', '/ios', '/ps4', '/ps5']
    linklist = []

    for platform in platformlist:

        try :
            url = BASE_URL+GAME_URL+DATE_URL+platform+SORT_URL+PAGE_URL+PAGE_NUM
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body = req.text, encoding = 'utf-8')
            lastpage = get_lastpage(res)
        except :
            pass

        for page in range(lastpage) :

            try :
                url = BASE_URL+GAME_URL+DATE_URL+platform+SORT_URL+PAGE_URL+str(page)
                req = requests.get(url, headers=header)
                res = TextResponse(req.url, body = req.text, encoding = 'utf-8')
                link = get_link(res)
                linklist += link
                print('success'+url)
            except :
                print('error')
    return linklist


def get_warning(res) :
    try :
        warning = res.xpath('//*[@class="review_top review_top_l"]/p/text()').extract()[0].strip().split(' - ')[0]
    except :
        try :
            warning = res.xpath('//*[@class="module errorcode_module error404_module"]//*[@class="error_code"]/text()').extract()[0]
        except :
            warning = ''
    return warning


def get_platform(tempurl) :
    try :
        platform = tempurl.split('/')[2]
    except :
        platform = ''
    return platform


def get_gamename(res) :
    gamename = res.xpath('//*[@class="product_title"]/a/h1/text()').extract()[0]
    return gamename


def get_userlist(res):
    userlist = []
    for i in range(1, 11):
        user = ''
        try:
            user = res.xpath(
                f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="name"]/span/text()').extract()[0]

        except:
            user = ''
        if len(user) < 1:
            try:
                user = res.xpath(
                    f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="name"]/a/text()'
                ).extract()[0]

            except:
                user = ''
                break

        if user != '':
            userlist.append(user)

    return userlist


def get_scorelist(res) :
    scorelist = res.xpath(
        '//*[@id="main"]/div[5]/div[2]/div/ol/li/div/div/div/div/div/div[1]/div[1]/div[2]/div/text()'
    ).extract()
    print(scorelist)
    return scorelist


def get_contentlist(res, scorelist):
    contentlist = []
    for i in range(1, len(scorelist) + 20):
    
        temp_contentlist = ''
        try:
            temp_contentlist = res.xpath(
                f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="blurb blurb_expanded"]/text()'
            ).extract()
            temp_content = ' '.join(temp_contentlist)
        except:
            temp_contentlist = ''
        if len(temp_content) < 1:
            try:
                temp_contentlist = res.xpath(
                    f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/span/text()').extract()
                temp_content = ' '.join(temp_contentlist)

                if len(temp_content) < 1:
                    try:
                        temp_contentlist = res.xpath(
                            f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/strong/text()').extract()
                        temp_content = ' '.join(temp_contentlist)
                    except:
                        pass
            except:
                try:
                    temp_contentlist = res.xpath(
                        f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/strong/text()').extract()
                    temp_content = ' '.join(temp_content)
                except:
                    pass
        if len(temp_content) < 1:
            try:
                temp_contentlist = \
                res.xpath(f'//*[@class="reviews user_reviews"]/li[{i}]//*[@class="review_body"]/text()').extract()[0]
                temp_content = temp_contentlist.strip()
                contentlist.append(temp_content)

            except:
                pass
        else:
            contentlist.append(temp_content)
    print(contentlist)
    return contentlist


def get_user(urllist):
    BASE_URL = 'https://www.metacritic.com'

    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}


    for link in urllist:
        tempurl = BASE_URL + link + '/user-reviews?page='
        platform = get_platform(link)
        try:
            # for i in range(500):
            for i in range(10):
                url = tempurl + str(i)
                # print(url)
                req = requests.get(url, headers=header)
                res = TextResponse(req.url, body=req.text, encoding='utf-8')

                if len(res.text) < 1:
                    url += '/'
                    req = requests.get(url, headers=header)
                    res = TextResponse(req.url, body=req.text, encoding='utf-8')

                    if len(res.text) < 1:
                        url += '/'
                        req = requests.get(url, headers=header)
                        res = TextResponse(req.url, body=req.text, encoding='utf-8')

                        if len(res.text) < 1:
                            url += '/'
                            req = requests.get(url, headers=header)
                            res = TextResponse(req.url, body=req.text, encoding='utf-8')

                warning = get_warning(res)
                print(res)
                if warning == 'There are no user reviews yet':
                    # print(url + ' is last page')
                    break
                elif warning == '404':
                    # print(url + ' is 404 ERROR')
                    break
                else:
                    try:
                        
                        gamename = get_gamename(res)
                        userlist = get_userlist(res)
                        scorelist = get_scorelist(res)
                        contentlist = get_contentlist(res, scorelist)
                        print(gamename, userlist, scorelist, contentlist)
                
                        for i in range(len(userlist)):
                            if len(userlist) == 0:
                                DOC = {"title":gamename, "platform":platform, "username":0, "userscore":0, "usercontet":0}
                            else:
                                DOC = {"title":gamename, "platform":platform, "username":userlist[i], "userscore":scorelist[i], "usercontet":contentlist[i]}
                            
                            producer.send(TOPIC_NAME, json.dumps(DOC).encode("utf-8"))
                            print(DOC)
                            producer.flush()

                    except:
                        print('ERROR!!!!!!!!!!!!!!' + url)

        except:
            pass


def metacrawl_meta():
    urllist = get_urllist()
    get_user(urllist)


metacrawl_meta()