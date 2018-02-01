package org.garret.perst.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.garret.perst.IterableIterator;
import org.garret.perst.Link;
import org.garret.perst.Query;
import org.garret.perst.Relation;

public class RelationImpl<M, O> extends Relation<M, O> {
  Link<M> link;

  RelationImpl() {}

  RelationImpl(StorageImpl db, O owner) {
    super(owner);
    link = new LinkImpl<M>(db, 8);
  }

  @Override
  public void add(int i, M obj) {
    link.add(i, obj);
  }

  @Override
  public boolean add(M obj) {
    return link.add(obj);
  }

  @Override
  public boolean addAll(Collection<? extends M> c) {
    return link.addAll(c);
  }

  @Override
  public boolean addAll(int index, Collection<? extends M> c) {
    return link.addAll(index, c);
  }

  @Override
  public boolean addAll(Link<M> anotherLink) {
    return link.addAll(anotherLink);
  }

  @Override
  public void addAll(M[] arr) {
    link.addAll(arr);
  }

  @Override
  public void addAll(M[] arr, int from, int length) {
    link.addAll(arr, from, length);
  }

  @Override
  public void clear() {
    link.clear();
  }

  @Override
  public boolean contains(Object obj) {
    return link.contains(obj);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return link.containsAll(c);
  }

  @Override
  public boolean containsElement(int i, M obj) {
    return link.containsElement(i, obj);
  }

  @Override
  public boolean containsObject(M obj) {
    return link.containsObject(obj);
  }

  @Override
  public void deallocateMembers() {
    link.deallocateMembers();
  }

  @Override
  public M get(int i) {
    return link.get(i);
  }

  @Override
  public Object getRaw(int i) {
    return link.getRaw(i);
  }

  @Override
  public int indexOf(Object obj) {
    return link.indexOf(obj);
  }

  @Override
  public int indexOfObject(Object obj) {
    return link.indexOfObject(obj);
  }

  @Override
  public void insert(int i, M obj) {
    link.insert(i, obj);
  }

  @Override
  public boolean isEmpty() {
    return link.isEmpty();
  }

  @Override
  public Iterator<M> iterator() {
    return link.iterator();
  }

  @Override
  public int lastIndexOf(Object obj) {
    return link.lastIndexOf(obj);
  }

  @Override
  public int lastIndexOfObject(Object obj) {
    return link.lastIndexOfObject(obj);
  }

  @Override
  public ListIterator<M> listIterator() {
    return link.listIterator();
  }

  @Override
  public ListIterator<M> listIterator(int index) {
    return link.listIterator(index);
  }

  @Override
  public void pin() {
    link.pin();
  }


  @Override
  public M remove(int i) {
    return link.remove(i);
  }

  @Override
  public boolean remove(Object o) {
    return link.remove(o);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return link.removeAll(c);
  }

  @Override
  public void removeObject(int i) {
    link.removeObject(i);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return link.retainAll(c);
  }

  @Override
  public IterableIterator<M> select(Class cls, String predicate) {
    Query<M> query = new QueryImpl<M>(getStorage());
    return query.select(cls, link.iterator(), predicate);
  }

  @Override
  public M set(int i, M obj) {
    return link.set(i, obj);
  }

  @Override
  public void setObject(int i, M obj) {
    link.setObject(i, obj);
  }

  @Override
  public void setSize(int newSize) {
    link.setSize(newSize);
  }

  @Override
  public int size() {
    return link.size();
  }

  @Override
  public List<M> subList(int fromIndex, int toIndex) {
    return link.subList(fromIndex, toIndex);
  }

  @Override
  public Object[] toArray() {
    return link.toArray();
  }

  @Override
  public <T> T[] toArray(T[] arr) {
    return link.<T>toArray(arr);
  }

  @Override
  public Object[] toRawArray() {
    return link.toRawArray();
  }

  @Override
  public void unpin() {
    link.unpin();
  }
}
