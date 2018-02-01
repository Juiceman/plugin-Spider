package org.garret.perst;

import java.util.Iterator;

public class IteratorWrapper<T> extends IterableIterator<T> 
{ 
    private Iterator<T> iterator;

    public IteratorWrapper(Iterator<T> iterator) { 
        this.iterator = iterator;
    }
    
    @Override
    public boolean hasNext() { 
        return iterator.hasNext();
    }
    
    @Override
    public T next() { 
        return iterator.next();
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}

