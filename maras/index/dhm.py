'''
A hash based index
'''
# Write sequence:
# 1. Get hash map file and file num
# 2. Write to associated storage file
# 3. Write to associated index file
# 4. Write to associated hash map file

# Import python libs
import struct
import os
import io

# Import maras libs
import maras.utils

# Import third party libs
import msgpack

HEADER_DELIM = '_||_||_'


def calc_position(key, hash_limit, bucket_size, header_len):
    '''
    Calculate the hash map file's key position
    '''
    return (abs(hash(key) & hash_limit) * bucket_size) + header_len


class DHM(object):
    '''
    Distributed Hash Map Index
    '''
    def __init__(
            self,
            db_root,
            hash_limit=0xfffff,
            key_hash='sha1',
            fmt='>KsQ',
            entry_map=None,
            header_len=1024,
            key_delim='/',
            open_fd=512,
            sync=True,
            **kwargs):
        if entry_map is None:
            entry_map = ['key', 'prev']
        self.entry_map = entry_map
        self.db_root = db_root
        self.hash_limit = hash_limit
        self.key_hash = key_hash
        self.hash_func, self.key_size = maras.utils.get_hash_data(key_hash)
        self.fmt = fmt.replace('K', str(self.key_size))
        self.bucket_size = self.__calc_bucket_size()
        self.header_len = header_len
        self.key_delim = key_delim
        self.open_fd = open_fd
        self.fds = []
        self.maps = {}
        self.sync = sync
        self.kwargs = kwargs

    def __calc_bucket_size(self):
        '''
        Calculate the size of the index buckets
        '''
        return len(struct.pack(self.fmt, '', 1))

    def _hm_dir(self, key):
        '''
        Return the hashmap directory
        '''
        key = key.strip(self.key_delim)
        root = key[:key.rfind(self.key_delim)].replace(self.key_delim, os.sep)
        return os.path.join(self.db_root, root)

    def _i_entry(self, key, id_, start, size, type_, prev, **kwargs):
        '''
        Contruct and return the index data entry as a serialized string
        '''
        entry = {
                'key': key,
                'st': start,
                'sz': size,
                'rev': maras.utils.gen_rev(),
                't': type_,
                'p': prev,
                }
        entry.update(kwargs)
        if not id_:
            entry['id'] = maras.utils.rand_hex_str(self.key_size)
        else:
            entry['id'] = id_
        packed = msgpack.dumps(entry)
        p_len = struct.pack('>H', len(packed))
        return '{0}{1}'.format(p_len, packed)

    def create_h_index(self, fn_):
        '''
        Create an index at the given location
        '''
        dirname = os.path.dirname(fn_)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        header = {
                'hash': self.key_hash,
                'h_limit': self.hash_limit,
                'header_len': self.header_len,
                'fmt': self.fmt,
                'bucket_size': self.bucket_size,
                'entry_map': self.entry_map,
                'dir': os.path.dirname(fn_),
                'num': int(fn_[fn_.rindex('_') + 1:]),
                }
        header_entry = '{0}{1}'.format(msgpack.dumps(header), HEADER_DELIM)
        try:
            fp_ = io.open(fn_, 'r+b')
        except IOError:
            fp_ = io.open(fn_, 'w+b')
        fp_.write(header_entry)
        header['fp'] = fp_
        return header

    def open_map(self, fn_):
        '''
        Attempt to open a map file, if the map file does not exist
        raise IOError
        '''
        if not os.path.isfile(fn_):
            raise IOError()
        fp_ = io.open(fn_, 'r+b')
        header = {'fp': fp_}
        raw_head = ''
        while True:
            raw_read = fp_.read(self.header_len)
            if not raw_read:
                raise ValueError('Hit the end of the index file with no header!')
            raw_head += raw_read
            if HEADER_DELIM in raw_head:
                header.update(
                        msgpack.loads(
                            raw_head[:raw_head.find(HEADER_DELIM)]
                            )
                        )
                return header

    def _get_h_entry(self, key, fn_):
        '''
        Return the hash map entry from the given file name.
        If the entry is not present then return None
        If the file is not present, create it
        '''
        if fn_ in self.maps:
            map_data = self.maps[fn_]
        else:
            try:
                map_data = self.open_map(fn_)
                self.maps[fn_] = map_data
            except IOError:
                map_data = self.create_h_index(fn_)
                self.maps[fn_] = map_data
        pos = calc_position(
                key,
                map_data['h_limit'],
                map_data['bucket_size'],
                map_data['header_len'])
        raw_h_entry = map_data['fp'].read(map_data['bucket_size'])
        try:
            comps = struct.unpack(map_data['fmt'], raw_h_entry)
        except Exception:
            comps = ('\0', 0)
        ret = {}
        ret['pos'] = pos
        for ind in range(len(map_data['entry_map'])):
            ret[map_data['entry_map'][ind]] = comps[ind]
        if comps[0] == '\0' * len(comps[0]):
            ret['key'] = key
            return ret, map_data
        return ret, map_data

    def hash_map_ref(self, key):
        '''
        Return the hash map reference data
        '''
        hmdir = self._hm_dir(key)
        f_num = 1
        while True:
            fn_ = os.path.join(hmdir, 'midx_{0}'.format(f_num))
            h_entry, map_data = self._get_h_entry(key, fn_)
            if not h_entry:
                # This is a new key
                break
            if key == h_entry['key']:
                # is the right key
                break
            f_num += 1
        return h_entry, fn_

    def insert(
            self,
            key,
            id_,
            start,
            size,
            type_,
            h_data,
            map_key,
            **kwargs):
        '''
        Insert the data into the specified location
        '''
        # 1. Get HT file data
        # 2. Get HT location
        # 3. Construct Index table data
        # 4. Write Index table data
        # 5. Construct hash table struct
        # 6. Write HT struct
        map_data = self.maps[map_key]
        i_entry = self._i_entry(
                key,
                id_,
                start,
                size,
                type_,
                h_data.get('prev', None),
                **kwargs)
        map_data['fp'].seek(0, 2)
        i_pos = map_data['fp'].tell()
        h_data['prev'] = i_pos
        map_data['fp'].write(i_entry)
        pack_args = []
        for ind in range(len(map_data['entry_map'])):
            pack_args.append(h_data[map_data['entry_map'][ind]])
        h_entry = struct.pack(map_data['fmt'], *pack_args)
        map_data['fp'].seek(h_data['pos'])
        map_data['fp'].write(h_entry)
