package org.garret.perst.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.garret.perst.IPersistentList;
import org.garret.perst.Link;
import org.garret.perst.PersistentCollection;
import org.garret.perst.Storage;

class ScalableList<E> extends PersistentCollection<E> implements IPersistentList<E> {
  static final int BTREE_THRESHOLD = 128;
  Link<E> small;

  IPersistentList<E> large;

  ScalableList() {}

  ScalableList(Storage storage, int initialSize) {
    super(storage);
    if (initialSize <= BTREE_THRESHOLD) {
      small = storage.<E>createLink(initialSize);
    } else {
      large = storage.<E>createList();
    }
  }

  @Override
  public boolean add(E o) {
    add(size(), o);
    return true;
  }

  @Override
  public void add(int i, E o) {
    if (small != null) {
      if (small.size() == BTREE_THRESHOLD) {
        large = getStorage().<E>createList();
        large.addAll(small);
        large.add(i, o);
        modify();
        small = null;
      } else {
        small.add(i, o);
      }
    } else {
      large.add(i, o);
    }
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    boolean modified = false;
    Iterator<? extends E> e = c.iterator();
    while (e.hasNext()) {
      add(index++, e.next());
      modified = true;
    }
    return modified;
  }

  @Override
  public void clear() {
    if (large != null) {
      large.clear();
    } else {
      small.clear();
    }
  }

  @Override
  public boolean contains(Object o) {
    return small != null ? small.contains(o) : large.contains(o);
  }

  @Override
  public E get(int i) {
    return small != null ? small.get(i) : large.get(i);
  }

  @Override
  public int indexOf(Object o) {
    return small != null ? small.indexOf(o) : large.indexOf(o);
  }

  @Override
  public boolean isEmpty() {
    return small != null ? small.isEmpty() : large.isEmpty();
  }

  @Override
  public Iterator<E> iterator() {
    return small != null ? small.iterator() : large.iterator();
  }

  @Override
  public int lastIndexOf(Object o) {
    return small != null ? small.lastIndexOf(o) : large.lastIndexOf(o);
  }

  @Override
  public ListIterator<E> listIterator() {
    return listIterator(0);
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    return small != null ? small.listIterator(index) : large.listIterator(index);
  }

  @Override
  public E remove(int i) {
    return small != null ? small.remove(i) : large.remove(i);
  }

  @Override
  public E set(int i, E obj) {
    return small != null ? small.set(i, obj) : large.set(i, obj);
  }

  @Override
  public int size() {
    return small != null ? small.size() : large.size();
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    return small != null ? small.subList(fromIndex, toIndex) : large.subList(fromIndex, toIndex);
  }

  @Override
  public Object[] toArray() {
    return small != null ? small.toArray() : large.toArray();
  }

  @Override
  public <T> T[] toArray(T a[]) {
    return small != null ? small.<T>toArray(a) : large.<T>toArray(a);
  }
}
