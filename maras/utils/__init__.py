'''
Misc utilities
'''

# Import python libs
import os
import binascii


def rand_hex_str(size):
    '''
    Return a random string of the passed size using hex encoding
    '''
    return binascii.hexlify(os.urandom(size/2))


def rand_raw_str(size):
    '''
    Return a raw byte string of the given size
    '''
    return os.urandom(size)
