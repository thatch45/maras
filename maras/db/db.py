'''
Simple database interface
'''

# Import python libs
import os

# Import maras libs
import maras.utils
import maras.index.hmap

# Import third party libs
import msgpack


class DB(object):
    '''
    A simple single threaded database
    '''
    def __init__(self, path, storage='msgpack', serial='msgpack'):
        self.dbpath = path
        self.path = os.path.join(path, 'maras.db')
        self.serial = serial
        self.header_len = 1000
        self.h_delim = '_||_||_'
        self.header = {'serial': serial}
        self.indexes = {}
        self.fp = self.__open_db()

    def __open_db(self):
        '''
        Auto create or open the index
        '''
        if not os.path.exists(self.path):
            return self.create()
        return self.open_db()

    def create(self):
        '''
        Create a new db file
        '''
        dbdir = os.path.dirname(self.path)
        if not os.path.exists(dbdir):
            os.makedirs(dbdir)

        if os.path.exists(self.path):
            raise ValueError('Database exists')
        fp_ = open(self.path, 'w+b')
        header = '{0}{1}'.format(msgpack.dumps(self.header), self.h_delim)
        fp_.write(header)
        return fp_

    def open_db(self):
        '''
        Open an existing index
        '''
        if not os.path.isfile(self.path):
            raise ValueError('No Database Exists')
        fp_ = open(self.path, 'rb')
        raw_head = fp_.read(self.header_len)
        self.header = msgpack.loads(raw_head[:raw_head.index(self.h_delim)])
        fp_.seek(0)
        return fp_

    def add_index(self, name):
        '''
        Add an index
        '''
        if name in self.indexes:
            raise ValueError('Already has index')
        ind = maras.index.hmap.HMapIndex(name, self.dbpath)
        self.indexes[name] = ind

    def insert(self, data, key, id_=None):
        '''
        Insert the given data into the db
        '''
        if not id_:
            id_ = maras.utils.rand_hex_str(40)
