import random
import threading

class SetAssociativeCache:
    def __init__(self, setCount, setSize, algo):
        self._setSize = setSize;
        self._setCount = setCount;
        self._cache = [{} for i in range(setCount) ];
        self._ralgo = self.lru
        self._wlock = threading.Lock()
        if not self.set_replacement_algo(algo):
            raise ValueError("Replacement Algo should be either LRU/MRU/user defined callable function.")

    def set_replacement_algo(self, algo):
        ret = False
        if callable(algo):
            self._ralgo = algo;
            ret = True
        elif isinstance(algo, str):
            algo = algo.upper()
            if algo.count("LRU") == 1:
                self._ralgo = self.lru;
                ret = True
            elif algo.count("MRU") == 1:
                self._ralgo = self.mru;
                ret = True
        return ret

    '''
    Gets the value associated with `key`.
    '''
    def get(self, key):
        value = None
        self._wlock.acquire()
        setIndex = self._getSetIndex(key)
        if key in self._cache[setIndex]:
            value = self._cache[setIndex].pop(key)
            self._cache[setIndex][key] = value
        self._wlock.release()
        return value

    '''
    Adds the `key` to the cache with the associated value. 
    '''
    def set(self, key, value):
        setIndex = self._getSetIndex(key)
        self._wlock.acquire()
        if key in self._cache[setIndex]:
            self._cache[setIndex].pop(key)
        elif len(self._cache[setIndex]) >= self._setSize:
            self._ralgo(self._cache[setIndex])
        
        if len(self._cache[setIndex]) >= self._setSize:
            self._wlock.release()
            raise Exception("Failed to remove an item from set")

        self._cache[setIndex][key] = value
        self._wlock.release()

    '''
    Returns `true` if the given `key` is present; otherwise, `false`.
    '''
    def containsKey(self, key):
        setIndex = self._getSetIndex(key);
        present = False
        if key in self._cache[setIndex]:
            present = True
        return present
    
    '''
    Maps a key to a set
    '''
    def _getSetIndex(self, key):
        random.seed(hash(key))
        return random.randint(0, self._setCount - 1)

    '''
    LRU algo for cache replacement
    '''
    def mru(self, set_):
        set_.popitem()

    '''
    MRU algo for cache replacement
    '''
    def lru(self, set_):
        del set_[list(set_)[0]]

