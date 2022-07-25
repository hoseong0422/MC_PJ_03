import pandas as pd
from bs4 import BeautifulSoup
import requests
import pymongo
import time
from fake_useragent import UserAgent

ua = UserAgent()
header = {'User-Agent': str(ua.random)}

USER = "user"
PWD = "pwd"
HOST = "host"
PORT = "port"
client = pymongo.MongoClient(f"mongodb://{USER}:{PWD}@{HOST}:{PORT}")
db = client['steam']

# api를 이용해 미리 받아둔 appid
df = pd.read_csv("appid.csv", encoding="utf-8")
appids = df["0"].values

def get_steam_game_info():
    
    for i in appids:
        
        URL = f"https://store.steampowered.com/app/{i}"

        res = requests.get(URL, headers=header)
        time.sleep(1)
        soup = BeautifulSoup(res.content, 'html.parser')
        
        try:
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
                "0"
        try:
            genre
        except:
            genre = "0"
        
        try:
            release_date
        except:
            release_date = "0"
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
            recent_reviews_temp = soup.select_one('#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc')
            # recent_reviews 평가 비율(최근 30일)
        except:
            recent_reviews_ratio = "0"
            recent_reviews_voted_users = "0"
        try:
            recent_reviews_ratio = soup.select_one("#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[1].replace("%","")
            if recent_reviews_ratio == "Need":
                recent_reviews_ratio = "0"
                
        except:
            recent_reviews_ratio = "0"
            # recent_reviews 점수를 준 유저 수(최근 30일)
        try:
            recent_reviews_voted_users = soup.select_one("#userReviews > div:nth-child(1) > div.summary.column > span.nonresponsive_hidden.responsive_reviewdesc").text.split()[4].replace(",","")
            if recent_reviews_voted_users == "reviews":
                recent_reviews_voted_users = "0"
        except:
            recent_reviews_voted_users = "0"
            
        # 전체기간 유저 평가 신작 게임은 없는 경우가 있다.
        try:
            all_reviews = soup.select_one("#userReviews > div:nth-child(2) > div.summary.column > span.game_review_summary.positive").text
        except:
            all_reviews = "0"
            
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

        #image link
        try:
            image_link = soup.select_one("img.game_header_image_full")["src"]
        except:
            image_link = "0"

        # grade
        try:
            grade = soup.select_one("div.game_rating_icon > img")["src"].split("/")[-1].split(".")[0]
        except:
            grade = "0"

        # info
        try:
            info_group = soup.select("div.label")
            info_list = [i.text for i in info_group]
        except:
            info_list = ["0"]

        # tag
        try:
            tag_group = soup.select("a.app_tag")
            tag_list = [i.text.strip() for i in tag_group]
        except:
            tag_list = ["0"]

        # about_this_game
        try:
            about_this_game = soup.select_one("#game_area_description").text.replace("\n\n"," ").replace("\n", " ")[16:].strip()
        except:
            about_this_game = "0"

        # language 부분만 추출
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
        # 아무것도 안들어잇으면 0 넣어주기
        if len(subtitles_list) == 0:
            subtitles_list = "0"
        
        DOC = {"appid":id, "title":title, "genre":genre, "developer":developer, "publisher":publisher,
            "franchise":franchise, "release_date":release_date, "recent_reviews":recent_reviews,
            "recent_reviews_ratio":int(recent_reviews_ratio), "recent_reviews_voted_users":int(recent_reviews_voted_users),
            "all_reviews":all_reviews,
            "all_reviews_ratio":int(all_reviews_ratio), "all_reviews_voted_users":int(all_reviews_voted_users),
            "image_link":image_link, "grade":grade, "info":info_list, "tag":tag_list, "about_this_game":about_this_game,
            "language_interface":interface_list, "language_fullaudio":fullaudio_list, "language_subtitles":subtitles_list}
        db.steam_info.insert_one(DOC)
        
        del id, title, genre, developer, publisher, franchise, release_date, recent_reviews,\
            info_list, tag_list, interface_list, fullaudio_list, subtitles_list, recent_reviews_ratio,\
            recent_reviews_voted_users, all_reviews, all_reviews_ratio, image_link, grade
    
get_steam_game_info()