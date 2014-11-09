'''
Storage using msgpack for serialization
'''

# Import python libs
import io
import os

# Import third party libs
import msgpack


class MPack(object):
    '''
    Store files using msgpack for data serialization
    '''
    def __init__(self, db_root):
        self.db_root = db_root
        self.stores = {}

    def get_stor(self, ind_ref):
        '''
        Get the stor data and fp based on the ind_ref
        '''
        fn_ = os.path.join(ind_ref['dir'], 'stor_{0}'.format(ind_ref['num']))
        if fn_ in self.stores:
            return self.stores[fn_]
        return self.add_stor(fn_)

    def add_stor(self, fn_):
        '''
        '''

    def insert(self, key, data, id_, ind_ref):
        '''
        '''
        stor_data = self.get_stor(ind_ref)
