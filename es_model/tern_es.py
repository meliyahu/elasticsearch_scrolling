
class TernEsSearch():
    def __init__(self, es_search_obj, es_index, dformat='json', result_size=20, search_filter=None,sort_field=None,scroll='2m', scroll_id=None):
        self._es_search_obj = es_search_obj
        self._es_index = es_index
        self._dformat = dformat
        self._result_size = result_size
        self._search_filter = search_filter
        self._sort_field = sort_field
        self._scroll = scroll
        self._scroll_id = scroll_id

    @property
    def es_search_obj(self):
        return self._es_search_obj

    @es_search_obj.setter
    def es_search_obj(self, value):
        self._es_search_obj = value
    
    @property
    def es_index(self):
        return self._es_index
    
    @es_index.setter
    def es_index(self, value):
        self._es_index = value

    @property
    def dformat(self):
        return self._dformat

    @dformat.setter
    def dformat(self, value):
        self._dformat = value

    @property
    def sort_field(self):
        return self._sort_field

    @sort_field.setter
    def sort_field(self, value):
        self._sort_field = value

    @property
    def search_filter(self):
        return self._search_filter

    @search_filter.setter
    def search_filter(self, value):
        self._search_filter = {} #value # helpers.generate_es_filter(value)

    @property
    def scroll_id(self):
        return self._scroll_id

    @scroll_id.setter
    def scroll_id(self, value):
        self._scroll_id = value

    def execute(self):
        """
        Main method
        :return:
        """
        if self.dformat == 'json':
            return self.get_data_as_json()
        else:
            return self.get_data_as_csv()

    def get_data_as_json(self):
        
        total_docs, data = self.perform_search()
        
        if data is None:
            return {"message": "No data found"}
        docs = []
        count = len(data)
        total_docs = 0
        for doc in data:
            # csv_doc = self.get_csv_friendly_doc(doc)
            docs.append(doc)
        return {"scroll_id": self.scroll_id,
               "total_docs": total_docs,
               "returned_docs": count,
               "data": docs
               }

    def get_data_as_csv(self):
        pass

    def perform_search(self):

        data = None
        total_docs = self.es_search_obj.count(index=self.es_index)

        if self.scroll_id is not None:
            data = self.es_search_obj.scroll(scroll_id=self.scroll_id, scroll=self._scroll) 
        else:
            # Initial scroll by search
            data = self.es_search_obj.search(
                index=self.es_index,
                scroll=self._scroll,
                size=self._result_size,
                body={}
                )
        if data is not None:
            self.scroll_id = data['_scroll_id']

        return total_docs, data
    
    def get_csv_friendly_doc(self, doc):
        """
        Recreate the json from elasticsearch by taking the key and the value of the
        json object and/or label fields in the sub object.
        Note: We are doing this because we need to create a csv file of the
        data. With sub json objects it is difficult to create individual
        columns in a csv for each field. This method flattens the json structure for
        csv data rendering.
        """
        new_doc = {}
        for f in doc:
            value = doc[f]["value"]
            label = doc[f].get('label', None)
            if label is not None:
                new_prop = f + '_uri'
                new_doc.update({new_prop: value})
                new_doc.update({f: label})
            else:
                new_doc.update({f: value})

        return new_doc


   