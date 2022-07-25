from kafka import KafkaProducer
import json
import requests
import scrapy
from scrapy.http import TextResponse
from fake_useragent import UserAgent

BROKERS = ["localhost:9092"]
TOPIC_NAME = "metacritic_info_topic"

producer = KafkaProducer(bootstrap_servers = BROKERS)

def get_lastpage(res):
    lastpage = int(res.xpath('//*[@class="page last_page"]/a/text()').extract()[0])
    print('lastpage calculated')
    return lastpage


def get_link(res):
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
    urllist = []

    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}

    lastpage = 0

    for platform in platformlist[:1]:

        try:
            url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + PAGE_NUM
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')
            lastpage = get_lastpage(res)

            print('success getting urllist')

        except:
            pass

        for page in range(10):

            try:
                url = BASE_URL + GAME_URL + DATE_URL + platform + SORT_URL + PAGE_URL + str(page)
                req = requests.get(url, headers=header)
                res = TextResponse(req.url, body=req.text, encoding='utf-8')
                link = get_link(res)
                urllist += link

                print('merged lastpage')
            except:
                pass


    return urllist


def get_title(res):
    try:
        title = res.xpath('//*[@class="product_title"]/a/h1/text()').extract()[0]
    except:
        title = ''
    return title

def get_platform(tempurl):
    try:
        platform = tempurl.split('/')[2]
    except:
        platform = ''
    return platform


def get_releasedate(res):
    try:
        releasedate = res.xpath('//*[@class="summary_detail release_data"]/span[2]/text()').extract()[0]
    except:
        releasedate = ''
    return releasedate


def get_metascore(res):
    try:
        metascore = \
        res.xpath('//*[@class="score_summary metascore_summary"]//*[@class="metascore_anchor"]/span/text()').extract()[
            0]
    except:
        try:
            metascore = res.xpath('//*[@itemprop="ratingValue"]/text()').extract()[0]
        except:
            metascore = ''
    return metascore


def get_metareviews(res):
    try:
        metareviews = res.xpath(
            '//*[@class="score_summary metascore_summary"]/div/div[2]/p/span[2]/a/span/text()'
        ).extract()[0].strip()

    except:
        metareviews = ''
    return metareviews


def get_userscore(res):
    try:
        userscore = res.xpath(
            '//*[@class="userscore_wrap feature_userscore"]//*[@class="metascore_anchor"]/div/text()'
        ).extract()[0]
    except:
        userscore = ''
    return userscore


def get_userreviews(res):
    try:
        userreviews = res.xpath(
            '//*[@class="score_summary"]//*[@class="summary"]/p/span[2]/a/text()'
        ).extract()[0].replace('Ratings', '').strip()
    except:
        userreviews = ''
    return userreviews


def get_summary(res):
    try:
        temp_summary = res.xpath('//*[@class="summary_detail product_summary"]/span[2]/span/text()').extract()
        summary = ''.join(temp_summary)
    except:
        summary = ''
    if len(summary) < 3:
        try:
            temp_summary = res.xpath(
                '//*[@class="summary_detail product_summary"]/span[2]/span/span[2]/text()').extract()
            summary = ''.join(temp_summary)
        except:
            summary = ''
    return summary


def get_developer(res):
    try:
        developer = res.xpath('//*[@class="summary_detail developer"]/span[2]/a/text()').extract()[0]
    except:
        try:
            developer = res.xpath('//*[@class="summary_detail developer"]/span[2]/span/text()').extract()[0]
        except:
            developer = ''
    return developer


def get_genre(res):
    try:
        temp_genre = res.xpath(
            '//*[@class="summary_detail product_genre"]/span/text()').extract()[1:]
        genre = ', '.join(temp_genre)
    except:
        genre = ''
    return genre


def get_players(res):
    try:
        players = res.xpath('//*[@class="summary_detail product_players"]/span[2]/text()').extract()[0]
    except:
        players = ''
    return players


def get_age(res):
    try:
        age = res.xpath(
            '//*[@class="summary_detail product_rating"]//*[@class="data"]/text()').extract()[0]
    except:
        age = ''
    return age


def get_gameinfo(urllist):
    BASE_URL = 'https://www.metacritic.com'
    fakeuser = UserAgent(verify_ssl=False).chrome
    header = {'User-Agent': fakeuser}

    title_list = []
    platform_list = []
    releasedate_list = []
    metascore_list = []
    metareviews_list = []
    userscore_list = []
    userreviews_list = []
    summary_list = []
    developer_list = []
    genre_list = []
    players_list = []
    age_list = []



    for tempurl in urllist[:5]:
        try:
            url = BASE_URL + tempurl
            req = requests.get(url, headers=header)
            res = TextResponse(req.url, body=req.text, encoding='utf-8')

            title_list.append(get_title(res))
            platform_list.append(get_platform(tempurl))
            releasedate_list.append(get_releasedate(res))
            metascore_list.append(get_metascore(res))
            metareviews_list.append(get_metareviews(res))
            userscore_list.append(get_userscore(res))
            userreviews_list.append(get_userreviews(res))
            summary_list.append(get_summary(res))
            developer_list.append(get_developer(res))
            genre_list.append(get_genre(res))
            players_list.append(get_players(res))
            age_list.append(get_age(res))

            print('SUCCESS' + tempurl)

        except:
            continue


    for i in range(len(title_list)):
        DOC = {"title":title_list[i], "platform":platform_list[i], "releasedata":releasedate_list[i],
                "metascore":metascore_list[i], "metareviews":metareviews_list[i],
                "userscore":userscore_list[i], "userreviews":userreviews_list[i],
                "summary":summary_list[i], "developer":developer_list[i],
                "genre":genre_list[i], "players":players_list[i], "age":age_list[i]}

        print(DOC)
        producer.send(TOPIC_NAME, json.dumps(DOC).encode("utf-8"))
        producer.flush()

    

def metacrawl_info():
    urllist = get_urllist()
    print('get_urllist result')
    print(urllist)
    get_gameinfo(urllist)

metacrawl_info()


    


