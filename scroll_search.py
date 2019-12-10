from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch, helpers
import logging
import json
import os

# Define config
host = os.environ.get('ES_HOST', "http://locahost:9200")
port = 80
timeout = 1000
index = "corveg_bioregions"
doc_type = "doc"
size = 5
body = {}

# Init Elasticsearch instance
es = Elasticsearch(hosts=[host], timeout=timeout)

# Process hits here


def process_hits(hits):
    print(f' hits = {len(hits)}')
    for item in hits:
        print(json.dumps(item, indent=2))

# Check index exists
# if not es.indices.exists(index=index):
#     print("Index " + index + " not exists")
#     exit()


# Init scroll by search
data = es.search(
    index=index,
    scroll='2m',
    size=size,
    body=body
)

# Get the scroll ID
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])
total = scroll_size

while scroll_size > 0:
    "Scrolling..."

    # Before scroll, process current batch of hits
    process_hits(data['hits']['hits'])

    data = es.scroll(scroll_id=sid, scroll='2m')

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])
    total += scroll_size

print(f'total is { total }')