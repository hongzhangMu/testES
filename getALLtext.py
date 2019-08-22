#coding:utf-8
import logging
import time
import requests
import json
import pymysql
from elasticsearch import Elasticsearch,helpers
import pymysql.cursors
from getclue import *
connection = pymysql.connect(host='192.168.124.108', port=13306, user='root', password='123456', db='MeetingTask-System', 
                             charset='utf8')
es_address = '192.168.124.243:19200'

while (1):

    while (1):
        orig_flag = 0
        des_flag = 1
        items = []
        # checkall = MtRecommend.query.filter(MtRecommend.flag == 0).limit(50).all()
        es = Elasticsearch(es_address)

        hash_list = []

        query = {
                "query": {
                    "bool": {
                      "must": [
                        {
                          "bool": {
                            "must": {
                              "exists": {
                                "field": "alltext"
                              }
                            }
                          }
                        }
                      ]
                    }
                  },

                "from": 0,
                "size": 2
            }
        res = es.search(index="mt", body=query)
        res = res['hits']['hits']
        data = []
        ids_res = []
        # log.info(res['hits']['hits'])
        for i in range(len(res)):
            alltext = getAlltext(res[i]['_source']['url'])
            print('get alltext')
            # tmp = {'title':res[i]['_source']['title'],'url':res[i]['_source']['url'],'text':res[i]['_source']['text'],'alltext':res[i]['_source']['alltext']}
            data.append(dict(title=res[i]['_source']['title'], url=res[i]['_source']['url'],
                             text=res[i]['_source']['text'], alltext=alltext))
        print("=================",dict(data=data))
        score = requests.post('http://192.168.126.189:8341/cluerecommend/itemScore', json=dict(data=data))
        # print("###########", score.json())
        score = score.json()['data']
        score_relevant = score[0]
        score_clue = score[1]
        tags = score[2]
        keywords = score[3]
        clauses = score[4]
        # topic_body = []
        # for i in data:
        #     topic_body.append(dict(title=i['title'], text=i['alltext'][:5000]))

        # topics = topic(topic_body)
        # print(topics)
        data_insert_es = []
        # print("#######", len(res), "######", len(data), "########", len(score_clue),"########", len(clauses))
        # print(asfdjkashdfjaskdfhasf)
        for i in range(len(res)):
            ids_res.append([res[i]['_id'], score_relevant[i], score_clue[i]])
            data_insert_es.append(dict(
                _index="mt",
                _id=res[i]['_id'],
                _source=dict(res[i]['_source'],
                             score_relevant=float(score_relevant[i]), score_clue=float(score_clue[i]),
                             score=float(score_relevant[i] + score_clue[i]), flag=des_flag, tags=",".join(tags[i]),
                             key_words=",".join(keywords[i]), clauses=clauses[i]),alltext=str(data[i]['alltext'])
            ))

        # filter(data_insert_es)

        helpers.bulk(es, data_insert_es)


        items = ids_res
        if len(items) != 0:
            print(items)
        else:
            print("没有更新的了 -_- ......")
            break
    print("本次完毕，100s后再次执行-----------")
    insertClue()
    time.sleep(100)
