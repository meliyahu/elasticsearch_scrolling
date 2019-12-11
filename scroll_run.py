from elasticsearch import Elasticsearch, helpers
from es_model.tern_es import TernEsSearch
import os
import json

host = os.environ.get('ES_HOST', "http://locahost:9200")
es = Elasticsearch(hosts=[host], timeout=1000)

tern_plot = TernEsSearch(es_search_obj=es, es_index='geology', dformat='json', result_size=20, search_filter=None, sort_field=None, scroll='2m', scroll_id=None)

data = tern_plot.execute()

for item in data['data']:
    print(json.dumps(item, indent=2))