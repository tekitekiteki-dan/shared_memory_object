from SharedObject import SharedObject
from collections import deque, defaultdict, OrderedDict

def test_set_shared_object():
    """Testing set attributes."""
    
    obj = {1, 2, 3}
    sh_obj1 = SharedObject(obj=obj, create=True, name='set_obj', is_nested=False)
    sh_obj2 = SharedObject(create=False, name='set_obj')
    assert obj == sh_obj1 == sh_obj2
    
    obj.add(1)
    sh_obj1.add(1)
    assert obj == sh_obj1 == sh_obj2
    
    obj.update(range(1000))
    sh_obj2.update(range(1000))
    assert obj == sh_obj1 == sh_obj2
    
    obj.discard(1)
    sh_obj1.discard(1)
    assert obj == sh_obj1 == sh_obj2
    
    obj.difference_update({4, 5})
    sh_obj1.difference_update({4, 5})
    assert obj == sh_obj1 == sh_obj2

    obj.intersection_update({2})
    sh_obj1.intersection_update({2})
    assert obj == sh_obj1 == sh_obj2

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2

def test_defaultdict_shared_object():
    """Testing defaultdict attributes."""
    
    obj = defaultdict(int, {1: 1, 2: {2: 2}, 3: 3})
    sh_obj1 = SharedObject(obj=obj, create=True, name='defd_obj', is_nested=True)
    sh_obj2 = SharedObject(create=False, name='defd_obj')
    assert obj == sh_obj1 == sh_obj2
    
    assert obj[4] == sh_obj1[4] == sh_obj2[4]

    obj[4] = 4
    sh_obj1[4] = 4
    assert obj == sh_obj1 == sh_obj2
    
    obj.update({i: i for i in range(10, 1000)})
    sh_obj2.update({i: i for i in range(10, 1000)})
    assert obj == sh_obj1 == sh_obj2
    
    obj.pop(2)
    sh_obj1.pop(2)
    assert obj == sh_obj1 == sh_obj2
    
    obj.popitem()
    sh_obj1.popitem()
    assert obj == sh_obj1 == sh_obj2

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2
