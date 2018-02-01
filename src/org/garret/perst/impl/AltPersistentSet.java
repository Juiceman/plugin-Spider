package org.garret.perst.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import org.garret.perst.IPersistentSet;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;

class AltPersistentSet<T> extends AltBtree<T> implements IPersistentSet<T> {
  AltPersistentSet() {
    type = ClassDescriptor.tpObject;
    unique = true;
  }

  AltPersistentSet(boolean unique) {
    type = ClassDescriptor.tpObject;
    this.unique = unique;
  }

  @Override
  public boolean isEmpty() {
    return nElems == 0;
  }

  @Override
  public boolean contains(Object o) {
    Key key = new Key(o);
    Iterator i = iterator(key, key, ASCENT_ORDER);
    return i.hasNext();
  }

  @Override
  public <E> E[] toArray(E[] arr) {
    return (E[]) super.toArray((T[]) arr);
  }

  @Override
  public boolean add(T obj) {
    return put(new Key(obj), obj);
  }

  @Override
  public boolean remove(Object o) {
    T obj = (T) o;
    return removeIfExists(new BtreeKey(checkKey(new Key(obj)), obj));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Set)) {
      return false;
    }
    Collection c = (Collection) o;
    if (c.size() != size()) {
      return false;
    }
    return containsAll(c);
  }

  @Override
  public int hashCode() {
    int h = 0;
    Iterator i = iterator();
    while (i.hasNext()) {
      h += getStorage().getOid(i.next());
    }
    return h;
  }

  @Override
  public IterableIterator<T> join(Iterator<T> with) {
    return with == null ? (IterableIterator<T>) iterator()
        : new JoinSetIterator<T>(getStorage(), iterator(), with);
  }
}
