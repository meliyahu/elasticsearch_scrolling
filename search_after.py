from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch, helpers
import logging
import connexion

log = logging.getLogger(__name__)

class ES:
    def __init__(self,client, index_name):
        self.client = client
        self.index_name = index_name

    def search(self, **kwargs):
        q = kwargs.get('q', '*')
        # sort = kwargs.get('sort', 'timestamp')
        sort = kwargs.get('sort', 'project_name.value')
        search_after = kwargs.get('search_after')
        print(f'search_after={search_after}')
        size = kwargs.get('size', 50)
        source = kwargs.get('source')
        extra = dict(
                size=size)

        if search_after:
            extra.update(dict(search_after=search_after))
        test = dict(search_after=search_after)
        print(f'test={test}')
        
        s = Search(using=self.client, index=self.index_name)
        if source:
            s = s.source(source)
        s = s.sort(sort)
        s = s.query(Q('query_string', query=q))
        s = s.extra(**extra)

        log.info('Query: %s', s.to_dict())

        r = s.execute()
        count = r.hits.total
        took = r.took

        result = r, count, took

        return result 

es_client = Elasticsearch(hosts=['https://es-test.tern.org.au'])

s = ES(es_client, 'site')

r, count, took = s.search(q='*', sort='project_name.value', search_after='[1463538857, "10000"]', size='5', source='site_id' )

print(f'count= {count}, took = {took}')
for doc in r:
    print(doc.to_dict())