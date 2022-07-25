from flask import Flask, request, render_template, jsonify
from sqlalchemy import create_engine, text
from gensim.models.doc2vec import Doc2Vec
from queries import *
from gentleman import *


app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    return "Hello Flask!!"

"""
POST 방식으로 요청 받은 appid을 모델에 넣어
추천받은 게임 6개를 numpy.int64 형식으로 응답
<numpy.int64>.item() -> numpy.int64를 int형식으로 변경해 줌
"""
@app.route('/recommendation/', methods=['POST'])
def recommend_by_app_id():
    
    data = request.json
    appid1 = int(data["appid1"])
    appid2 = int(data["appid2"])
    appid3 = int(data["appid3"])
    
    # appids : 넘파이 리스트로 추천된 앱아이디 6개
    appids = gentleman_ver1(appid1, appid2, appid3)

    rec_appid1 = appids[0].item()
    rec_appid2 = appids[1].item()
    rec_appid3 = appids[2].item()
    rec_appid4 = appids[3].item()
    rec_appid5 = appids[4].item()
    rec_appid6 = appids[5].item()

    appid_return = {"recomented_games" : 
                        [{"title1" : get_title(rec_appid1)['title'].replace("  ", " "), "appid1" : str(rec_appid1)}, 
                        {"title2" : get_title(rec_appid2)['title'].replace("  "," "), "appid2" : str(rec_appid2)},
                        {"title3" : get_title(rec_appid3)['title'].replace("  "," "), "appid3" : str(rec_appid3)},
                        {"title4" : get_title(rec_appid4)['title'].replace("  "," "), "appid4" : str(rec_appid4)},
                        {"title5" : get_title(rec_appid5)['title'].replace("  "," "), "appid5" : str(rec_appid5)},
                        {"title6" : get_title(rec_appid6)['title'].replace("  "," "), "appid6" : str(rec_appid6)}]}
    return jsonify(appid_return)

"""
게임 선택 화면
action, adventure, casual, indie, racing, rpg, simulation, sports, strategy
각 장르별 탑 15개를 미리 VIEW로 만들어 놓은 뒤 조회하는 방식으로 구현
"""
@app.route('/select/', methods=['POST'])
def select():
    action_return = get_action_top15()
    adventure_return = get_adventure_top15()
    casual_return = get_casual_top15()
    indie_return = get_casual_top15()
    racing_return = get_racing_top15()
    rpg_return = get_rpg_top15()
    simulation_return = get_simulation_top15()
    sports_return = get_sports_top15()
    strategy_return = get_strategy_top15()
    
    select_return = {"returns" :
                [{"action_top15" : action_return}, 
                {"adventure_top15" : adventure_return}, 
                {"casual_top15" : casual_return},
                {"indie_top15" : indie_return},
                {"racing_top15": racing_return},
                {"rpg_top15" : rpg_return},
                {"simulation_top15" : simulation_return},
                {"sports_top15" : sports_return},
                {"strategy_top15" : strategy_return}]}
    return jsonify(select_return)

if __name__ == '__main__':
    app.run(debug=True, port=3000)
