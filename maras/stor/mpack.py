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

    def get_stor(self, map_):
        '''
        Get the stor data and fp based on the ind_ref
        '''
        fn_ = os.path.join(map_['dir'], 'stor_{0}'.format(map_['num']))
        if fn_ in self.stores:
            return self.stores[fn_]
        return self.add_stor(fn_)

    def add_stor(self, fn_):
        '''
        Open up a new storage file and add it in
        '''
        stor_dir = os.path.dirname(fn_)
        if not os.path.exists(stor_dir):
            os.makedirs(stor_dir)
        try:
            fp_ = io.open(fn_, 'r+b')
        except IOError:
            fp_ = io.open(fn_, 'w+b')
        self.stores[fn_] = fp_
        return fp_

    def insert(self, key, data, id_, ind_ref):
        '''
        '''
        stor = self.get_stor(ind_ref)
        stor_str = self.data_in(data, id_)
        stor.seek(0, 2)
        start = stor.tell()
        size = len(stor_str)
        stor.write(stor_str)
        return start, size

    def get(self, ind_ref, map_):
        '''
        Get the referenced data out of the storage file
        '''
        stor = self.get_stor(map_)
        stor.seek(ind_ref['start'])
        raw = stor.read(ind_ref['size'])
        return self.data_out(raw)

    def data_in(self, data, id_):
        '''
        Serialize the data as it is sent in
        '''
        in_data = {'d': data, 'id_': id_}
        return msgpack.dumps(in_data)

    def data_out(self, raw):
        '''
        Return the processed data
        '''
        return msgpack.loads(raw)
