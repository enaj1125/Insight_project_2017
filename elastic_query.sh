curl -u elastic -XGET 'localhost:9200/twitter_indice_dl/_search?pretty' -H 'Content-Type: application/json' -d'
{
  "query": { "range": {
                "ttimes": {
                        "from": "2015-05-02"
                }
         }
 }
}
'
