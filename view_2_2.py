from app import app
from flask import jsonify
from elasticsearch import Elasticsearch
from flask import render_template, request

# Setting up
hosts = ['ip-172-31-1-8']
es = Elasticsearch(
                        hosts,
                        port=9200,
                        http_auth=('elastic', 'changeme'),
                        verify_certs=False,
                        sniff_on_start=True,    # sniff before doing anything
                        sniff_on_connection_fail=True,    # refresh nodes after a node fails to respond
                        sniffer_timeout=60, # and also every 60 seconds
                        timeout=15
                        )

@app.route('/')
@app.route('/email')
def email():
 return render_template("email.html")

@app.route('/email', methods = ['POST'])
def email_post():
    search_text = request.form["emailid"]
    search_date = request.form["date"]

    response = es.search(index='twitter_data_2_2', body={
"query": {
   "bool": {
     "filter":[
       {"match": { "ttext": search_text }},
       {"range":{
          "ttimes":{"gte": search_date}} }
       ]
     }
    },
"size" : 25 })

    if response['timed_out'] == True:
        json_response =[]
    else:
        search_results = response['hits']['hits']
        json_response = [X['_source'] for X in search_results ]

