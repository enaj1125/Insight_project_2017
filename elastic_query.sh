
curl -u elastic -XGET 'localhost:9200/test_7/_search?pretty' -H 'Content-Type: application/json' -d'
{
  "query": { "range": {
                "timeStamp": {
                        "from": "2017/09/02"
                }
         }
 }
}
'
