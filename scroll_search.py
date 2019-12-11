from elasticsearch_dsl import Search, Q
from elasticsearch import Elasticsearch, helpers
import logging
import json
import os
import time

# Define config
host = os.environ.get('ES_HOST', "http://locahost:9200")
port = 80
timeout = 1000
# index = "geology"
index = "corveg_bioregions"
doc_type = "doc"
size = 1000
# body = {"query": {
#     "query_string": {
#       "query": "(geology_unit_method.label:cut OR geology_unit_method.label:map) AND geology_type_result.label:sand"
#     }
#   }}
body = {}

# Init Elasticsearch instance
es = Elasticsearch(hosts=[host], timeout=timeout)

# Process hits here

def get_formatted_elapsed_time(start_time, end_time):
    """
    Nicely formats elapsed time in hours, minutes, seconds
    :param start_time:
    :param end_time:
    :return:
    """
    hours, rem = divmod(end_time - start_time, 3600)
    minutes, seconds = divmod(rem, 60)
    result = "{:0>2} hours {:0>2} minutes {:05.2f} seconds".format(int(hours), int(minutes), seconds)

    return result
    
def process_hits(hits):
    print(f' hits = {len(hits)}')
    for item in hits:
        print(json.dumps(item, indent=2))

# Check index exists
# if not es.indices.exists(index=index):
#     print("Index " + index + " not exists")
#     exit()

start_time = time.time()
# Init scroll by search
data = es.search(
    index=index,
    scroll='2m',
    size=size,
    body=body
)

count = es.count(index=index)   
print(f'count = {count}')

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
print(f'Took {get_formatted_elapsed_time(start_time, time.time())}')