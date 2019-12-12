from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch, helpers
import logging
import connexion
import json
import os

log = logging.getLogger(__name__)

class ES:
    def __init__(self,client, index_name):
        self.client = client
        self.index_name = index_name

    def search(self, **kwargs):
        q = kwargs.get('q', '*')
        sort = kwargs.get('sort', 'timestamp')
        sort = kwargs.get('sort', '[id]')
        search_after = kwargs.get('search_after')
        size = kwargs.get('size', 20)
        source = kwargs.get('source')
        extra = dict(size=size)

        if search_after:
            print(f'search_after= {search_after}')
            extra.update(dict(search_after=search_after))
        
        s = Search(using=self.client, index=self.index_name)
        if source:
            s = s.source(source)
        
        # print(f'sort={sort}')
        
        s = s.sort(*sort)
        
        # s = s.sort('site_sample_type.label.keyword', '_id')
        s = s.query(Q('query_string', query=q))
        s = s.extra(**extra)

        # log.info('Query: %s', s.to_dict())

        response = s.execute()
        rec_count = response.hits.total.value
        # print(f'rec_count = {rec_count}')
        took = response.took
        return response, rec_count, took 

    def process_hits(self, docs):
        pass
        # for doc in docs:
        #     print(doc.to_dict())


host = os.environ.get('ES_HOST', "http://locahost:9200")
es_client = Elasticsearch(hosts=[host])

es_obj = ES(es_client, 'site')

response, rec_count, took = es_obj.search(q='*', sort=['site_sample_type.label.keyword', '_id'], search_after=None, size='1000', source=None)

# print(f'count= {count}, took = {took}')
# print(f' r = {json.dumps(r.hits.hits[0].to_dict(), indent=2, sort_keys=True)}')
print(f' next sort={response.hits.hits[0]["sort"]}')
next_page = response.hits.hits[0]["sort"]
count = len(response)
while count > 0:
    "Paginate.."
    es_obj.process_hits(response)
    search_after = response.hits.hits[0]["sort"]
    # response, rec_count, took = es_obj.search(q='*', sort=['site_sample_type.label.keyword', '_id'], search_after=['Belt Transect', 'corveg-site-24070'], size='1', source=None)
    response, rec_count, took = es_obj.search(q='*', sort=['site_sample_type.label.keyword', '_id'], search_after=[*search_after], size='1000', source=None)
    count = len(response)
    print(f'count = {count}')






