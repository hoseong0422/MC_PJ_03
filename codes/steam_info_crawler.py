import pandas as pd
from bs4 import BeautifulSoup
import requests
import pymongo
import time
from fake_useragent import UserAgent

ua = UserAgent()
header = {'User-Agent': str(ua.random)}


def __get_mongodb_collection(collection_name):
    """
    Args:
        collection_name (string): 컬랙션 이름을 아규먼트로 입력합니다.
    Returns:
        collection : 수집된 데이터를 저장할 컬랙선을 리턴합니다.
    """
    USER = "user"
    PWD = "pwd"
    HOST = "host"
    PORT = "port"
    client = pymongo.MongoClient(f"mongodb://{USER}:{PWD}@{HOST}:{PORT}")
    db = client['steam']
    collection = db[collection_name]

    return collection


# api를 이용해 미리 받아둔 appid
df = pd.read_csv("appid.csv", encoding="utf-8")
appids = df["0"].to_list()

def get_steam_game_info():
    """
    스팀 게임정보를 가져오는 함수입니다.
    게임마다 각 요소들이 없는 경우도 있기 때문에 예외처리를 이용하여 요소가 없는 경우 "0"으로 저장하거나 다음 appid로 넘어가게 됩니다.
    """
    
    for i in appids:
        
        URL = f"https://store.steampowered.com/app/{i}"

        res = requests.get(URL, headers=header)
        time.sleep(1)
        soup = BeautifulSoup(res.content, 'html.parser')
        
        try:
            # 게임에 이름, 장르, 개발사 등 주요 정보가 표시되는 화면입니다. 
            # 게임이 아닐경우 에러가 나는 것으로 확인되어 해당 요소가 없다면 다음 appid로 for문이 넘어가게 됩니다.
            detail = soup.select_one("#genresAndManufacturer").text
        except:
            continue
        
        #appid
        id = appids[i]

        temp =  detail.split("\n")

        for i in range(len(temp)):
            if temp[i][:5] == "Genre":
                genre = temp[i][7:]

            elif temp[i][:5] == "Title":
                title = temp[i][7:]

            elif temp[i][:9] == "Developer":
                developer = temp[i+1]

            elif temp[i][:9] == "Publisher":
                publisher = temp[i+1]

            elif temp[i][:9] == "Franchise":
                franchise = temp[i+1]
            
            try:    
                if temp[i][:12] == "Release Date":
                    release_date = temp[i][14:]
            except:
                release_date = "0"

        """
        해당 요소가 있는지 없는지 예외처리를 이용하여 해당 변수에 값이 할당된 적 없다면 
        "0"을 저장하게 하는 예외처리 입니다.
        """
        try:
            genre
        except:
            genre = "0"
        
        try: 
            publisher
        except:
            publisher = "0"
        
        try:
            developer
        except:
            developer = "0"

        try: 
            franchise
        except:
            franchise = "0"

        try:
            recent_reviews = soup.select_one("#userReviews > div:nth-child(1) > div.summary.column > span").text
        except:
            recent_reviews = "0"
            
        try:    
            """
            recent_reviews 최근 30일동안 유저들이 어떻게 평가했는지에 대한 정보입니다.
            신규 게임의 경우 평가가 없기 때문에 해당 요소가 없습니다.
            따라서 해당 요소가 없을 경우 예외처리를 이용하여 30일 내 평가에 해당되는 내용이 "0"으로 저장됩니다.
            
            recent_reviews_ratio : 30일 기간동안 해당 평가를 한 유저의 비율
            recent_reviews_voted_users : 30일 기간동안 해당 평가를 한 유저의 수
            """
            recent_reviews_temp = soup.select_one('#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc')
        except:
            recent_reviews_ratio = "0"
            recent_reviews_voted_users = "0"
        try:
            recent_reviews_ratio = soup.select_one("#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[1].replace("%","")
            # 최근 출시한 게임이여서 정보가 부족한 경우 Need라고 표시되는 경우가 있어서 이 경우도 "0"으로 저장되도록 하였음
            if recent_reviews_ratio == "Need":
                recent_reviews_ratio = "0"
                
        except:
            recent_reviews_ratio = "0"
        try:
            recent_reviews_voted_users = soup.select_one("#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[4].replace(",","")
            # 최근 출시한 게임이여서 정보가 부족한 경우 reviews라고 표시되는 경우가 있어서 이 경우도 "0"으로 저장되도록 하였음
            if recent_reviews_voted_users == "reviews":
                recent_reviews_voted_users = "0"
        except:
            recent_reviews_voted_users = "0"
            
        # 전체기간 유저 평가 신작 게임은 없는 경우가 있다.
        try:
            all_reviews = soup.select_one("#userReviews > div:nth-child(2) > div.summary.column > span.game_review_summary.positive").text
        except:
            all_reviews = "0"
        
        """
        all_reviews 게임이 출시된 이후 전체 기간동안 유저들이 어떻게 평가했는지에 대한 정보입니다.
        신규 게임의 경우 평가가 없기 때문에 해당 요소가 없습니다.
        따라서 해당 요소가 없을 경우 예외처리를 이용하여 30일 내 평가에 해당되는 내용이 "0"으로 저장됩니다.
        
        all_reviews_ratio : 전체 기간동안 해당 평가를 한 유저의 비율
        all_reviews_voted_users : 전체 기간동안 해당 평가를 한 유저의 수
        """    
        try:
            all_reviews_temp = soup.select_one('#userReviews > div:nth-child(2) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc')
        except:
            all_reviews_ratio = "0"
            all_reviews_voted_users = "0"
        try:
            # 전체기간 유저 평가 비율
            all_reviews_ratio = soup.select_one("#userReviews > div:nth-child(2) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[1].replace("%", "")
        except:
            all_reviews_ratio = "0"
            # 전체기간 유저 점수를 준 유저 수
        try:
            all_reviews_voted_users = soup.select_one("#userReviews > div:nth-child(2) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[4].replace(",","")
        except:
            all_reviews_voted_users = "0"

        """
        게임 헤더 이미지 링크를 가져오는 부분입니다.
        웹서비스 구현시에 게임 정보페이지에서 활용될 수 있는 내용이라 수집하였습니다.
        """
        try:
            image_link = soup.select_one("img.game_header_image_full")["src"]
        except:
            image_link = "0"

        """
        게임 연령 등급 가져오는 부분입니다.
        웹서비스 구현시에 게임 정보페이지에서 활용될 수 있는 내용이라 수집하였습니다.
        """
        try:
            grade = soup.select_one("div.game_rating_icon > img")["src"].split("/")[-1].split(".")[0]
        except:
            grade = "0"

        """
        게임 정보(싱글 플레이, 멀티 플레이)가 담겨 있는 부분입니다.
        """
        try:
            info_group = soup.select("div.label")
            info_list = [i.text for i in info_group]
        except:
            info_list = ["0"]

        """
        게임의 태그 정보가 담겨있는 부분입니다.
        게임의 타이틀 외에 검색시 사용되는 부분이라 수집하였습니다.
        """
        try:
            tag_group = soup.select("a.app_tag")
            tag_list = [i.text.strip() for i in tag_group]
        except:
            tag_list = ["0"]

        """
        게임에 대한 설명 부분입니다.
        웹서비스 구현 시 표시하기 위해 수집하였으나
        한글로 수집이 가능한 url 패스 파라미터를 발견하여 추천페이지에서 수집하여 표시 할 예정입니다.
        """
        try:
            about_this_game = soup.select_one("#game_area_description").text.replace("\n\n"," ").replace("\n", " ")[16:].strip()
        except:
            about_this_game = "0"

        """
        지원 언어표시 부분 수집
        language_interface : 인터페이스 지원 언어
        language_fullaudio : 오디오 지원 언어
        language_subtitles : 자막 지원 언어
        """
        language_group = soup.select("#languageTable > table > tr")
        parsed_language = language_group[1:]
        parsed_language

        # language에 표시된 언어들 추출
        language_list = []
        for i in parsed_language:
            language_list.append(i.select_one("td.ellipsis").text.strip())

        # interface를 지원하는 언어들
        interface_list = []
        for i in range(len(language_list)):
            try:
                if parsed_language[i].select_one("td.checkcol > span").text == "✔":
                    interface_list.append(language_list[i])
            except:
                interface_list = ["0"]

        # fullaudio를 지원하는 언어들
        fullaudio_list = []
        for i in range(len(language_list)):
            try:
                if parsed_language[i].select_one("td.checkcol").next_sibling.next_sibling.text.strip() == "✔":
                    fullaudio_list.append(language_list[i])
            except:
                continue
        if len(fullaudio_list) == 0:
            fullaudio_list.append("0")

        # subtitle을 지원하는 언어들
        subtitles_list = []
        for i in range(len(language_list)):
            try:
                if parsed_language[i].select_one("td.checkcol").next_sibling.next_sibling.next_sibling.next_sibling.text.strip() == '✔':
                    subtitles_list.append(language_list[i])
            except:
                continue
        # 아무것도 없다면 0 넣어주기
        if len(subtitles_list) == 0:
            subtitles_list = "0"
        
        DOC = {"appid":id, "title":title, "genre":genre, "developer":developer, "publisher":publisher,
            "franchise":franchise, "release_date":release_date, "recent_reviews":recent_reviews,
            "recent_reviews_ratio":int(recent_reviews_ratio), "recent_reviews_voted_users":int(recent_reviews_voted_users),
            "all_reviews":all_reviews,
            "all_reviews_ratio":int(all_reviews_ratio), "all_reviews_voted_users":int(all_reviews_voted_users),
            "image_link":image_link, "grade":grade, "info":info_list, "tag":tag_list, "about_this_game":about_this_game,
            "language_interface":interface_list, "language_fullaudio":fullaudio_list, "language_subtitles":subtitles_list}
        
        collection = __get_mongodb_collection("steam_info")
        collection.insert_one(DOC)
        
        del id, title, genre, developer, publisher, franchise, release_date, recent_reviews,\
            info_list, tag_list, interface_list, fullaudio_list, subtitles_list, recent_reviews_ratio,\
            recent_reviews_voted_users, all_reviews, all_reviews_ratio, image_link, grade
    
get_steam_game_info()