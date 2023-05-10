from collections import deque, defaultdict, OrderedDict
import pickle, gc
from multiprocessing.shared_memory import SharedMemory
from functools import partial

data_types = (set, list, dict, deque, defaultdict, OrderedDict)


def apply_changes_dec(func):
    
    def wrapper(self, *args, **kwargs):
        self.apply_changes()
        res = func(self, *args, **kwargs)
        return res
    wrapper.__name__ = func.__name__
    
    return wrapper


def write_changes_dec(func):
    
    def wrapper(self, *args, **kwargs):
        
        if self._is_nested:
            varnames = func.__code__.co_varnames
            
            if 'item' in varnames:
                item_index = varnames.index('item') - 1
                item = args[item_index]
                
                if isinstance(item, data_types):
                    item = SharedObject(
                        obj = item,
                        create = True,
                        size = self.size,
                        is_nested = True,
                        shm_register = self._shm_register
                    )
                    self._shm_register.add(item.name)
                    args = list(args)
                    args[item_index] = item
        
        res = func(self, *args, **kwargs)
        self._write_changes(func.__name__, *args, **kwargs)
        return res
    wrapper.__name__ = func.__name__

    return wrapper


class SharedObject:
    """Wrapper for python mutable container objects which uses shared memory as a backend."""
    
    # DUNDER METHODS #
    
    def __init__(self, obj = None, create = None, name = None, size = 10_000,
                 serializer = pickle, is_nested = None, shm_register = None, control_shm_size = 1000):
        
        if obj is None and create == True:
            raise Exception('If create == True, obj is need to be specified')
        
        if obj is not None:
            if not isinstance(obj, data_types):
                raise Exception(f'Object type `{type(obj).__name__}` is not in the supported types {[data_type.__name__ for data_type in data_types]}')
        
        self._update_stream_position = 0
        self._full_dump_counter = 0
        self.closed = False
        
        self._control = SharedMemory(create = create, name = name, size = control_shm_size)
        self.name = self._control.name

        self._update_stream_position_remote = self._control.buf[  0:   4]
        self._full_dump_counter_remote      = self._control.buf[  4:   8]
        self._is_nested_remote              = self._control.buf[  8:  10]
        self._full_dump_memory_name_remote  = self._control.buf[ 10: 265]
        self._obj_type_remote               = self._control.buf[265:1000]
        
        self._serializer = serializer

        self._buffer = SharedMemory(create = create, name = f'{self.name}_memory', size = size)
        self.size = self._buffer.size
        
        self.unlinked = False
        self._full_dump_memory = None

        if create:
    
            if isinstance(obj, defaultdict):
                obj_type = partial(defaultdict, obj.default_factory, {})
            elif isinstance(obj, deque):
                obj_type = partial(deque, (), obj.maxlen)
            else:
                obj_type = type(obj)

            obj_type_remote = self._serializer.dumps(obj_type)
                
            if 265 + len(obj_type_remote) > self._control.size:
                raise Exception(f'Not enough shared memory to save obj type, increase `control_shm_size` to {265+len(obj_type_remote)}')
            
            self._obj_type_remote[:4] = len(obj_type_remote).to_bytes(4, 'little')
            self._obj_type_remote[4:4+len(obj_type_remote)] = obj_type_remote
            
            if isinstance(obj, set):
                is_nested = False
            
            if is_nested:
                self._is_nested_remote[:1] = b'1'
        else:
            
            obj_type_length = int.from_bytes(bytes(self._obj_type_remote[:4]), 'little')
            obj_type = pickle.loads(self._obj_type_remote[4:4+obj_type_length])
            
            is_nested = self._is_nested_remote[:1] == b'1'
        
        self._obj_type = obj_type
        self._is_nested = is_nested

        if self._is_nested:
            if shm_register is not None: 
                self._shm_register = shm_register
            else:
                if create:
                    self._shm_register = SharedObject(obj=set(), create=True, name=f'{self.name}_register',
                                                    is_nested=False)
                else:
                    self._shm_register = SharedObject(create=False, name=f'{self.name}_register')
        else:
            self._shm_register = None
        
        self.data = self._obj_type()
        
        if obj is not None:  
            if isinstance(self.data, (list, deque)):
                self.extend(obj)
            else:
                self.update(obj)
        else:
            self.apply_changes()
        
    def __del__(self):
        if not self.closed:
            self.close()
    
    @apply_changes_dec
    def __repr__(self):
        return repr(self.data)
    
    @apply_changes_dec
    def __str__(self):
        return str(self.data)
    
    @apply_changes_dec
    def __getitem__(self, key):
        return self.data[key]
    
    @apply_changes_dec
    @write_changes_dec
    def __setitem__(self, key, item):
        self.data[key] = item
    
    @apply_changes_dec
    @write_changes_dec
    def __delitem__(self, key):
        del self.data[key]
        
    @apply_changes_dec
    def __contains__(self, key):
        return key in self.data

    @apply_changes_dec
    def __len__(self):
        return len(self.data)

    @apply_changes_dec
    def __iter__(self):
        return iter(self.data)
    
    @apply_changes_dec
    def __reduce__(self):
        return (partial(self.__class__, name = self.name, shm_register = self._shm_register), ())
    
    @apply_changes_dec
    def __eq__(self, other):
        return self.data == other
    
    def __hash__(self):
        return hash(self.data)
    
    # LIST / DEQUE METHODS #
    
    @apply_changes_dec
    @write_changes_dec
    def append(self, item):
        self.data.append(item)
    
    @apply_changes_dec
    @write_changes_dec
    def clear(self):
        self.data.clear()
    
    @apply_changes_dec
    def copy(self):
        if len(self.data) > 0 and self._is_nested:
            if isinstance(self.data, (list, deque)):
                tmp = [item.copy() if type(item) == SharedObject else item \
                             for item in self.data]
            else:
                tmp = {key: item.copy() if type(item) == SharedObject else item \
                             for key, item in self.data.items()}
            return self._obj_type(tmp)
        else:
            return self.data.copy()
    
    @apply_changes_dec
    def count(self, item):
        return self.data.count(item)
    
    @apply_changes_dec
    def extend(self, other):
        for item in other:
            self.append(item)
    
    @apply_changes_dec
    def index(self, item, start=0, stop=9223372036854775807):
        return self.data.index(item, start, stop)
    
    @apply_changes_dec
    @write_changes_dec
    def insert(self, index, item):
        self.data.insert(index, item)
    
    @apply_changes_dec
    @write_changes_dec
    def pop(self, *args):
        return self.data.pop(*args)
    
    @apply_changes_dec
    @write_changes_dec
    def remove(self, item):
        self.data.remove(item)
    
    @apply_changes_dec
    @write_changes_dec
    def reverse(self):
        self.data.reverse()
    
    @apply_changes_dec
    @write_changes_dec
    def sort(self, key=None, reverse=False):
        self.data.sort(key=key, reverse=reverse)

    # DEQUE METHODS #
    
    @apply_changes_dec
    @write_changes_dec
    def appendleft(self, item):
        self.data.appendleft(item)
    
    @apply_changes_dec
    def extendleft(self, other):
        for item in other:
            self.appendleft(item)
    
    @apply_changes_dec
    @write_changes_dec
    def popleft(self):
        return self.data.popleft()
    
    @apply_changes_dec
    @write_changes_dec
    def rotate(self, n=1):
        self.data.rotate(n)
    
    # DICT / DEFAULTDICT / ORDEREDDICT METHODS #
    
    @apply_changes_dec
    def get(self, key, default=None):
        return self.data.get(key, default)
    
    @apply_changes_dec
    def items(self):
        return self.data.items()
    
    @apply_changes_dec
    def keys(self):
        return self.data.keys()
    
    @apply_changes_dec
    def values(self):
        return self.data.values()
    
    @apply_changes_dec
    def update(self, other):
        if isinstance(self.data, (dict, defaultdict, OrderedDict)):
            for key, item in other.items():
                self[key] = item
        else:
            for item in other:
                self.add(item)
        
    @apply_changes_dec
    @write_changes_dec
    def popitem(self, *args):
        return self.data.popitem(*args)
    
    @apply_changes_dec
    @write_changes_dec
    def setdefault(self, key, item=None):
        self.data.setdefault(key, item)
    
    # ORDEREDDICT METHODS #
    
    @apply_changes_dec
    @write_changes_dec
    def move_to_end(self, key, last=True):
        self.data.move_to_end(key, last)
    
    # SET METHODS #
    
    @apply_changes_dec
    @write_changes_dec
    def add(self, item):
        self.data.add(item)
    
    @apply_changes_dec
    def difference(self, *args):
        return self.data.difference(*args)
    
    @apply_changes_dec
    @write_changes_dec
    def difference_update(self, *args):
        self.data.difference_update(*args)
    
    @apply_changes_dec
    @write_changes_dec
    def discard(self, item):
        self.data.discard(item)
    
    @apply_changes_dec
    def intersection(self, *args):
        return self.data.intersection(*args)
    
    @apply_changes_dec
    @write_changes_dec
    def intersection_update(self, *args):
        self.data.intersection_update(*args)
    
    @apply_changes_dec
    def isdisjoint(self, other):
        return self.data.isdisjoint(other)
    
    @apply_changes_dec
    def issubset(self, other):
        return self.data.issubset(other)
    
    @apply_changes_dec
    def issuperset(self, other):
        return self.data.issuperset(other)
    
    @apply_changes_dec
    def symmetric_difference(self, other):
        return self.data.symmetric_difference(other)
    
    @apply_changes_dec
    @write_changes_dec
    def symmetric_difference_update(self, other):
        self.data.symmetric_difference_update(other)
    
    @apply_changes_dec
    def union(self, *args):
        return self.data.union(*args)
    
    # SHARED MEMORY METHODS #
    
    def apply_changes(self):
        """Apply changes to object from shared memory stream."""

        if self._full_dump_counter < int.from_bytes(self._full_dump_counter_remote, 'little'):
            self._load_full_object(force = True)

        if self._update_stream_position < int.from_bytes(self._update_stream_position_remote, 'little'):
            
            pos = self._update_stream_position
            while pos < int.from_bytes(self._update_stream_position_remote, 'little'):
                length = int.from_bytes(bytes(self._buffer.buf[pos:pos+4]), 'little')
                pos += 4
                func_name, args, kwargs = self._serializer.loads(bytes(self._buffer.buf[pos:pos+length]))
                self.data.__getattribute__(func_name)(*args, **kwargs)
                pos += length
                self._update_stream_position = pos
    
    def _load_full_object(self, force = False):
        """Load full dump of data from shared memory."""
        
        full_dump_counter = int.from_bytes(self._full_dump_counter_remote, 'little')
        
        if force or (self._full_dump_counter < full_dump_counter):
            name = bytes(self._full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')
            full_dump_memory = SharedMemory(create = False, name = name)

            length = int.from_bytes(bytes(full_dump_memory.buf[:4]), 'little')
            self.data = self._serializer.loads(bytes(full_dump_memory.buf[4:4+length]))
            self._full_dump_counter = full_dump_counter
            self._update_stream_position = 0

            full_dump_memory.close()
        else:
            raise Exception("Cannot load full dump, no new data available")
    
    def _write_changes(self, func_name, *args, **kwargs):
        """Write applied changes to data to shared memory."""
        
        marshalled = self._serializer.dumps((func_name, args, kwargs))
        length = len(marshalled)

        start_position = int.from_bytes(self._update_stream_position_remote, 'little')
        end_position = start_position + 4 + length
        
        if end_position > self.size:
            self.dump_full_object()
            return

        marshalled = length.to_bytes(4, 'little') + marshalled

        self._buffer.buf[start_position:end_position] = marshalled

        self._update_stream_position = end_position
        self._update_stream_position_remote[:] = end_position.to_bytes(4, 'little')
    
    def dump_full_object(self):
        """Dump full data to shared memory."""
        
        self.apply_changes()
        
        prev_dump_name = bytes(self._full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')
        
        marshalled = self._serializer.dumps(self.data)
        length = len(marshalled)

        full_dump_memory = SharedMemory(create = True, size = length + 4)

        if length + 4 > full_dump_memory.size:
            raise

        full_dump_memory.buf[:4] = length.to_bytes(4, 'little')
        full_dump_memory.buf[4:4+length] = marshalled

        full_dump_memory.close()

        self._full_dump_memory_name_remote[:] = full_dump_memory.name.encode('utf-8').ljust(255)

        self._full_dump_counter += 1
        current = int.from_bytes(self._full_dump_counter_remote, 'little')
        self._full_dump_counter_remote[:] = int(current + 1).to_bytes(4, 'little')

        self._update_stream_position = 0
        self._update_stream_position_remote[:] = b'\x00\x00\x00\x00'

        if prev_dump_name and prev_dump_name != full_dump_memory.name:
            self.unlink_shm_by_name(prev_dump_name)

        self._full_dump_memory = full_dump_memory

        return full_dump_memory
    
    def close(self):
        """Close all the instances of shared memory."""
        
        if self.closed == True:
            return True
        self._close_all_shm_objects()
        self.closed = True
        return True
    
    def _close_all_shm_objects(self):
        """Close all the instances of shared memory."""
        
        self._del_remotes()
        gc.collect()
        if self._full_dump_memory:
            self._full_dump_memory.close()
        self._control.close()
        self._buffer.close()
        if len(self.data) > 0 and self._is_nested:
            if isinstance(self.data, (list, deque)):
                for item in self.data:
                    if isinstance(item, SharedObject):
                        item.close()
            else:
                for item in self.data.values():
                    if isinstance(item, SharedObject):
                        item.close()
    
    def _del_remotes(self):
        """Delete all the saved parts of control shared memory."""
        
        del self._update_stream_position_remote
        del self._full_dump_counter_remote
        del self._is_nested_remote
        del self._full_dump_memory_name_remote
        del self._obj_type_remote
    
    def unlink(self):
        """Unlink all the instances of shared memory."""
        
        if self.unlinked == True:
            return True
        self._unlink_all_shm_objects()
        self.unlinked = True
        return True
    
    def _unlink_all_shm_objects(self):
        """Unlink all the instances of shared memory."""
        
        self._unlink_shm_object_by_name(self.name, self._shm_register)
        if self._is_nested:
            if self._shm_register is not None:
                if len(self._shm_register) > 0:
                    for name in self._shm_register.data:
                        self._unlink_shm_object_by_name(name, self._shm_register)
                self._shm_register.close()
                self._unlink_shm_object_by_name(f'{self.name}_register')
    
    def _unlink_shm_object_by_name(self, name, shm_register = None):
        """Unlink all the instances of shared memory related to particular object."""
        
        shm_object = SharedObject(create = False, name = name, shm_register = shm_register)
        shm_object._control.unlink()
        shm_object._buffer.unlink()
        if int.from_bytes(shm_object._full_dump_counter_remote, 'little') > 0:
            name = bytes(shm_object._full_dump_memory_name_remote).decode('utf-8').strip().strip('\x00')
            SharedObject.unlink_shm_by_name(name)
        shm_object.close()
        
    @staticmethod
    def unlink_shm_by_name(name):
        """Delete shared memory by its name."""
        
        try:
            shm = SharedMemory(create = False, name = name)
            shm.unlink()
            shm.close()
            return True
        except Exception:
            return False