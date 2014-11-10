'''
Simple database interface
'''
# Write Seq:
# 1. Get index data for key
# 2. write storage using key index data
# 3. write index using storage data return
# Import python libs
import os
import io

# Import maras libs
import maras.utils
import maras.index.hmap
import maras.stor.mpack

# Import third party libs
import msgpack


class DB(object):
    '''
    A simple single threaded database
    '''
    def __init__(
            self,
            path,
            storage='msgpack',
            serial='msgpack'):
        self.dbpath = path
        self.path = os.path.join(path, 'maras_meta.db')
        self.serial = serial
        self.header_len = 1024
        self.h_delim = '_||_||_'
        self.header = {}
        self.indexes = {}
        self.default_storage = maras.stor.mpack.MPack(self.dbpath)
        self.stores = {}
        self.stores[storage] = self.default_storage

    def create(
            self,
            hash_limit=0xfffff,
            key_hash='sha1',
            fmt='>KsQ',
            entry_map=None,
            header_len=1024,
            key_delim='/',
            open_fd=512,
            sync=True):
        '''
        Create a new db, this will create the new database meta file, the
        meta file contains the default information to apply to new indexes
        allowing the database to be re-opened without needing to re-pass
        all of the index params
        '''
        if os.path.exists(self.path):
            raise ValueError('Database exists')
        dbdir = os.path.dirname(self.path)
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)
        if entry_map is None:
            entry_map = ['key', 'prev']
        self.header['hash_limit'] = hash_limit
        self.header['key_hash'] = key_hash
        self.header['fmt'] = fmt
        self.header['entry_map'] = entry_map
        self.header['header_len'] = header_len
        self.header['key_delim'] = key_delim
        self.header['open_fd'] = open_fd
        self.header['sync'] = sync
        with io.open(self.path, 'w+b') as fp_:
            header = '{0}{1}'.format(msgpack.dumps(self.header), self.h_delim)
            fp_.write(header)
        return self.header

    def open_db(self):
        '''
        Open an existing index
        '''
        if not os.path.isfile(self.path):
            raise ValueError('No Database Exists')
        with io.open(self.path, 'rb') as fp_:
            raw_head = fp_.read(self.header_len)
            self.header = msgpack.loads(raw_head[:raw_head.index(self.h_delim)])
        return self.header

    def add_index(self, name):
        '''
        Add an index
        '''
        if name in self.indexes:
            raise ValueError('Already has index')
        ind = maras.index.dhm.DHM(name, self.dbpath, **self.header)
        self.indexes[name] = ind

    def insert(self, data, key, id_=None, stor='msgpack'):
        '''
        Insert the given data into the db
        '''
        if stor is None:
            stor = self.stores.get(stor, self.default_storage)
        if not id_:
            id_ = maras.utils.rand_hex_str(64)
        for index in self.indexes:
            ind_ref, map_key = index.hash_map_ref(key)
            size, start = stor.insert(key, data, id_, ind_ref)
            index.insert(key, id_, start, size, None, ind_ref, map_key)
            ind_ref['start'] = start
            ind_ref['size'] = size
            return ind_ref
