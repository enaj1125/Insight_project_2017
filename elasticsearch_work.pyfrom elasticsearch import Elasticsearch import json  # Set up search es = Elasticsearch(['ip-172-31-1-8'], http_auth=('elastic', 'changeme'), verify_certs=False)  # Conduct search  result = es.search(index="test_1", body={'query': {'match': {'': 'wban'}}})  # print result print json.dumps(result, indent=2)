from elasticsearch import Elasticsearch
import json

# Set up search
es = Elasticsearch(['ip-172-31-1-8'], http_auth=('elastic', 'changeme'), verify_certs=False)

# Conduct search 
result = es.search(index="test_1", body={'query': {'match': {'': 'wban'}}})

# print result
print json.dumps(result, indent=2)
