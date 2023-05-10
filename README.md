# Shared Memory Object

Wrapper for python mutable container objects which uses shared memory as a backend.

## TODO:

Issues:
* The error raises after executing 'obj.popitem() == sh_obj.popitem()'. obj - built-in python types instance, sh_obj - SharedObject instance. The operation 'popitem' is made twice for 'sh_obj'.

Features:
* Add 'from_keys' method for dict-like objects.