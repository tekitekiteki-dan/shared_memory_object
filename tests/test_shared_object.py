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
    
    removed_obj = obj.discard(1)
    removed_sh_obj = sh_obj1.discard(1)
    assert removed_obj == removed_sh_obj
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


def test_list_shared_object():
    """Testing defaultdict attributes."""

    obj = [0, 1, [2, 2], 3]
    sh_obj1 = SharedObject(obj=obj, create=True, name='list_obj', is_nested=True)
    sh_obj2 = SharedObject(create=False, name='list_obj')
    assert obj == sh_obj1 == sh_obj2

    assert obj[1] == sh_obj1[1] == sh_obj2[1]

    obj.append([4, 4])
    sh_obj2.append([4, 4])
    assert obj == sh_obj1 == sh_obj2
    
    assert obj.copy() == sh_obj1.copy() == sh_obj2.copy()

    obj.clear()
    sh_obj1.clear()
    assert obj == sh_obj1 == sh_obj2

    obj.extend(range(1000))
    sh_obj1.extend(range(1000))
    assert obj == sh_obj1 == sh_obj2

    obj.insert(5, [4, 4])
    sh_obj2.insert(5, [4, 4])
    assert obj == sh_obj1 == sh_obj2

    removed_obj = obj.remove([4 ,4])
    removed_sh_obj = sh_obj1.remove([4, 4])
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    obj.reverse()
    sh_obj1.reverse()
    assert obj == sh_obj1 == sh_obj2

    removed_obj = obj.pop(3)
    removed_sh_obj = sh_obj1.pop(3)
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    obj.sort(reverse=True)
    sh_obj1.sort(reverse=True)
    assert obj == sh_obj1 == sh_obj2

    assert obj.index(10) == sh_obj1.index(10) == sh_obj2.index(10)

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2


def test_deque_shared_object():
    """Testing deque attributes."""

    obj = deque([0, 1, [2, 2], 3], maxlen=4)
    sh_obj1 = SharedObject(obj=obj, create=True, name='deq_obj', is_nested=True)
    sh_obj2 = SharedObject(create=False, name='deq_obj')
    assert obj == sh_obj1 == sh_obj2

    obj.appendleft([-1, -1])
    sh_obj1.appendleft([-1, -1])
    assert obj == sh_obj1 == sh_obj2

    removed_obj = obj.popleft()
    removed_sh_obj = sh_obj1.popleft()
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    obj.rotate(1)
    sh_obj1.rotate(1)
    assert obj == sh_obj1 == sh_obj2

    obj.extendleft(range(10))
    sh_obj1.extendleft(range(10))

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2
    

def test_dict_shared_object():
    """Testing dict attributes."""

    obj = {1: 1, 2: {2: 2}, 3: 3}
    sh_obj1 = SharedObject(obj=obj, create=True, name='dict_obj', is_nested=True)
    sh_obj2 = SharedObject(create=False, name='dict_obj')
    assert obj == sh_obj1 == sh_obj2

    obj[4] = {4: 4}
    sh_obj2[4] = {4: 4}
    assert obj == sh_obj1 == sh_obj2

    assert obj[4] == sh_obj1[4] == sh_obj2[4]

    assert obj.get(5, 5) == sh_obj1.get(5, 5) == sh_obj2.get(5, 5)

    obj.setdefault(5, 5)
    sh_obj2.setdefault(5, 5)
    assert obj[5] == sh_obj1[5] == sh_obj2[5]

    assert obj.copy() == sh_obj1.copy() == sh_obj2.copy()

    obj.clear()
    sh_obj1.clear()
    assert obj == sh_obj1 == sh_obj2

    obj.update({i: i for i in range(1000)})
    sh_obj1.update({i: i for i in range(1000)})
    assert obj == sh_obj1 == sh_obj2

    assert list(obj.values()) == list(sh_obj1.values()) == list(sh_obj2.values())

    assert obj.keys() == sh_obj1.keys() == sh_obj2.keys()
    assert list(obj.values()) == list(sh_obj1.values()) == list(sh_obj2.values())
    assert obj.items() == sh_obj1.items() == sh_obj2.items()

    sh_obj2.unlink()
    del sh_obj1
    del sh_obj2


def test_default_dict_shared_object():
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
    
    removed_obj = obj.pop(2)
    removed_sh_obj = sh_obj1.pop(2)
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    assert sorted(obj, reverse=True) == sorted(sh_obj1, reverse=True) == sorted(sh_obj2, reverse=True)

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2

def test_ordered_dict_object():
    """Testing OrderedDict attributes."""

    obj = OrderedDict({1: 1, 2: {2: 2}, 3: 3})
    sh_obj1 = SharedObject(obj=obj, create=True, name='ordd_obj', is_nested=True)
    sh_obj2 = SharedObject(create=False, name='ordd_obj')
    assert obj == sh_obj1 == sh_obj2

    removed_obj = obj.pop(2)
    removed_sh_obj = sh_obj1.pop(2)
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2
    
    removed_obj = obj.popitem()
    removed_sh_obj = sh_obj1.popitem()
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    obj[2] = {2: 2}
    sh_obj1[2] = {2: 2}
    assert obj == sh_obj1 == sh_obj2

    obj.move_to_end(2, False)
    sh_obj2.move_to_end(2, False)
    assert obj == sh_obj1 == sh_obj2

    removed_obj = obj.popitem(False)
    removed_sh_obj = sh_obj1.popitem(False)
    assert removed_obj == removed_sh_obj
    assert obj == sh_obj1 == sh_obj2

    sh_obj1.unlink()
    del sh_obj1
    del sh_obj2