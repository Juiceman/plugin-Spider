package org.garret.perst.impl;
import  org.garret.perst.*;
import java.util.*;

class HashSetImpl<T> extends PersistentCollection<T> implements IPersistentSet<T> { 
    IPersistentHash<T,T> map;

    HashSetImpl(StorageImpl storage) { 
        super(storage);
        map = storage.createHash();
    }

    HashSetImpl() {}

    @Override
    public boolean isEmpty() { 
        return size() != 0;
    }

    @Override
    public int size() { 
        return map.size();
    }

    @Override
    public void clear() { 
        map.clear();
    }
    
    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }
    
    @Override
    public Object[] toArray() { 
        return map.values().toArray();
    }
  
    @Override
    public <E> E[] toArray(E a[]) { 
        return map.values().toArray(a);
    }
    
    @Override
    public Iterator<T> iterator() { 
        return map.values().iterator();
    }
    
    @Override
    public boolean add(T obj) { 
        if (map.containsKey(obj)) { 
            return false;
        }
        map.put(obj, obj);
        return true;
    }

    @Override
    public boolean remove(Object o) { 
        return map.remove(o) != null;
    }
    
    @Override
    public void deallocate() { 
        map.deallocate();
        super.deallocate();
    }

    @Override
    public IterableIterator<T> join(Iterator<T> with) { 
        return with == null ? (IterableIterator<T>)iterator() : new JoinSetIterator<T>(getStorage(), iterator(), with);
    }        
}
    
