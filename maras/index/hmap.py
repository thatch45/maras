'''
A hash based index
'''
# Import python libs
import struct
import os

# Import maras libs
import maras.utils

# Import third party libs
import msgpack


class HMapIndex(object):
    '''
    Hash index
    '''
    def __init__(
            self,
            name,
            dbpath,
            hash_limit=0xfffff,
            key_hash='sha1',
            header_len=1000):
        self.dbpath = dbpath
        self.path = os.path.join(dbpath, '{0}.index'.format(name))
        self.hash_limit = hash_limit
        self.key_hash, self.key_size = maras.utils.get_hash_data(key_hash)
        self.header = {'hlim': hash_limit,
                       'keyh': key_hash,
                       'ksz': self.key_size,
                       'nsz': 8}
        self.header_len = header_len
        self.h_delim = '_||_||_'
        self.fp = self.__open_index()
        self.h_bucket_fmt, self.h_bucket_size = self.__gen_bucket_fmt()

    def __gen_hbucket_fmt(self):
        '''
        Generate the hash bucket struct format based on the sizes in the
        header
        '''
        # Match msgpack big edian
        # hbucket format is:
        # key, data_location, next
        # key is length of the hash function
        # data_location and next are longs
        fmt = '>s{0}LL'.format(self.key_size)
        test_struct = struct.pack(fmt, maras.util.rand_hex_str(self.key_size), 0, 0)
        return fmt, len(test_struct)

    def __open_index(self):
        '''
        Auto create or open the index
        '''
        if not os.path.exists(self.path):
            return self.create()
        return self.open_index()

    def create(self):
        '''
        Create a new index
        '''
        if os.path.exists(self.path):
            raise ValueError('Index exists')
        fp_ = open(self.path, 'w+b')
        header = '{0}{1}'.format(msgpack.dumps(self.header), self.h_delim)
        fp_.write(header)
        return fp_

    def open_index(self):
        '''
        Open an existing index
        '''
        if not os.path.isfile(self.path):
            raise ValueError('No Index Exists')
        fp_ = open(self.path, 'rb')
        raw_head = fp_.read(self.header_len)
        self.header = msgpack.loads(raw_head[:raw_head.index(self.h_delim)])
        self.hash_limit = self.header['hlim']
        self.key_hash, self.key_size = maras.utils.get_hash_data(self.header['keyh'])
        fp_.seek(0)
        return fp_

    def _hash_position(self, key, first):
        '''
        Calculate the position of the hash based on the key and start location
        '''
        return abs(hash(key) & self.hash_limit) * self.h_bucket_size + first

    def _get_h_entry(self, pos):
        '''
        Return the unpacked tuple if it exists, else None
        '''
        self.fp.seek(pos)
        raw = self.fp.read(self.h_bucket_size)
        try:
            return struct.unpack(self.h_bucket_fmt, raw)
        except Exception:
            return None

    def _find_h_tail(self, rec_top, entry):
        '''
        Use the entry to find the end of the linked list of referenced h_refs
        '''
        while entry[2]:
            rec_top = entry[2]
            entry = self._get_entry(entry[2])
        return rec_top, entry

    def _write_h_entry(self, h_pos, key, id_, start, size, next_):
        '''
        Write the hash entry
        '''
        top = self._write_d_entry(id_, start, size)
        h_entry = struct.pack(self.h_bucket_fmt, key, top, next_)
        self.fp.seek(h_pos)
        self.fp.write(h_entry)

    def _write_collision(self, entry, h_pos, key, id_, start, size):
        '''
        '''
        top = self._write_d_entry(id_, start, size)
        # find the tail
        tail_pos, tail = self._find_h_tail(h_pos, entry)
        tail_entry = struct.pack(self.h_bucket_fmt, tail[0], tail[1], top)
        self.fp.seek(tail_pos)
        self.fp.write(tail_entry)
        self.fp.seek(0, 2)
        h_entry = struct.pack(self.h_bucket_fmt, key, top, 0)
        self.fp.write(h_entry)

    def _write_d_entry(self, id_, start, size):
        '''
        Write the data ref entry
        '''
        self.fp.seek(0, 2)
        if self.fp.tell() < self.header_len:
            self.fp.seek(self.header_len)
        top = self.fp.tell()
        self.fp.write(struct.pack(self.h_bucket_fmt, id_, start, size))
        return top

    def insert(self, key, id_, start, size):
        '''
        Insert the data into the specified location
        '''
        if not id_:
            id_ = maras.utils.rand_hex_str(self.key_size)
        h_pos = self._hash_position(key, self.header_len)
        entry = self._get_entry(h_pos)
        if entry is None:
            self._write_h_entry(h_pos, key, id_, start, size, 0)
        elif key != entry[0]:
            # hash_collision
            self._write_collision(entry, h_pos, key, id_, start, size)
        return True
