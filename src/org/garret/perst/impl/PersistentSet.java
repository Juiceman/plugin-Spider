package org.garret.perst.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.garret.perst.IPersistentSet;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;
import org.garret.perst.PersistentIterator;
import org.garret.perst.Storage;

class JoinSetIterator<T> extends IterableIterator<T> implements PersistentIterator {
  private PersistentIterator i1;
  private PersistentIterator i2;
  private int currOid;
  private Storage storage;

  JoinSetIterator(Storage storage, Iterator<T> left, Iterator<T> right) {
    this.storage = storage;
    i1 = (PersistentIterator) left;
    i2 = (PersistentIterator) right;
  }

  @Override
  public boolean hasNext() {
    if (currOid == 0) {
      int oid1, oid2 = 0;
      while ((oid1 = i1.nextOid()) != 0) {
        while (oid1 > oid2) {
          if ((oid2 = i2.nextOid()) == 0) {
            return false;
          }
        }
        if (oid1 == oid2) {
          currOid = oid1;
          return true;
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return (T) storage.getObjectByOID(currOid);
  }

  @Override
  public int nextOid() {
    return hasNext() ? currOid : 0;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}


class PersistentSet<T> extends Btree<T> implements IPersistentSet<T> {
  PersistentSet(boolean unique) {
    type = ClassDescriptor.tpObject;
    this.unique = unique;
  }

  PersistentSet() {}

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
    return removeIfExists(new BtreeKey(checkKey(new Key(obj)), getStorage().getOid(obj)));
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
