package org.garret.perst.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.garret.perst.Assert;
import org.garret.perst.IPersistent;
import org.garret.perst.IPersistentSet;
import org.garret.perst.Index;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;
import org.garret.perst.PersistentCollection;
import org.garret.perst.PersistentIterator;
import org.garret.perst.Relation;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

class ThickIndex<T> extends PersistentCollection<T> implements Index<T> {
  Index<Object> index;
  int nElems;

  static final int BTREE_THRESHOLD = 128;

  ThickIndex(StorageImpl db, Class keyType) {
    super(db);
    index = db.<Object>createIndex(keyType, true);
  }

  ThickIndex() {}

  private final T getFromRelation(Object s) {
    if (s == null) {
      return null;
    }
    if (s instanceof Relation) {
      Relation r = (Relation) s;
      if (r.size() == 1) {
        return (T) r.get(0);
      }
    }
    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
  }

  @Override
  public T get(Key key) {
    return getFromRelation(index.get(key));
  }

  @Override
  public T get(Object key) {
    return getFromRelation(index.get(key));
  }

  @Override
  public ArrayList<T> getList(Key from, Key till) {
    return extendList(index.getList(from, till));
  }

  @Override
  public ArrayList<T> getList(Object from, Object till) {
    return extendList(index.getList(from, till));
  }

  @Override
  public Object[] get(Key from, Key till) {
    return extend(index.get(from, till));
  }

  @Override
  public Object[] get(Object from, Object till) {
    return extend(index.get(from, till));
  }

  private ArrayList<T> extendList(ArrayList s) {
    ArrayList<T> list = new ArrayList<T>();
    for (int i = 0, n = s.size(); i < n; i++) {
      list.addAll((Collection<T>) s.get(i));
    }
    return list;
  }


  protected Object[] extend(Object[] s) {
    ArrayList list = new ArrayList();
    for (int i = 0; i < s.length; i++) {
      list.addAll((Collection) s[i]);
    }
    return list.toArray();
  }

  public T get(String key) {
    return get(new Key(key));
  }

  @Override
  public Object[] getPrefix(String prefix) {
    return extend(index.getPrefix(prefix));
  }

  @Override
  public ArrayList<T> getPrefixList(String prefix) {
    return extendList(index.getPrefixList(prefix));
  }

  @Override
  public Object[] prefixSearch(String word) {
    return extend(index.prefixSearch(word));
  }

  @Override
  public ArrayList<T> prefixSearchList(String word) {
    return extendList(index.prefixSearchList(word));
  }

  @Override
  public int size() {
    return nElems;
  }

  @Override
  public void clear() {
    for (Object p : index) {
      ((IPersistent) p).deallocate();
    }
    index.clear();
    nElems = 0;
    modify();
  }

  @Override
  public Object[] toArray() {
    return extend(index.toArray());
  }

  @Override
  public <E> E[] toArray(E[] arr) {
    ArrayList<E> list = new ArrayList<E>();
    for (Object c : index) {
      list.addAll((Collection<E>) c);
    }
    return list.toArray(arr);
  }

  static class ExtendIterator<E> extends IterableIterator<E> implements PersistentIterator {
    @Override
    public boolean hasNext() {
      return inner != null;
    }

    @Override
    public E next() {
      if (inner == null) {
        throw new NoSuchElementException();
      }
      E obj = inner.next();
      if (!inner.hasNext()) {
        if (outer.hasNext()) {
          inner = ((Iterable<E>) outer.next()).iterator();
        } else {
          inner = null;
        }
      }
      return obj;
    }

    @Override
    public int nextOid() {
      if (inner == null) {
        return 0;
      }
      int oid = ((PersistentIterator) inner).nextOid();
      while (oid == 0) {
        if (outer.hasNext()) {
          inner = ((Iterable<E>) outer.next()).iterator();
          oid = ((PersistentIterator) inner).nextOid();
        } else {
          inner = null;
          break;
        }
      }
      return oid;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    ExtendIterator(IterableIterator<?> iterable) {
      this(iterable.iterator());
    }

    ExtendIterator(Iterator<?> iterator) {
      outer = iterator;
      if (iterator.hasNext()) {
        inner = ((Iterable<E>) iterator.next()).iterator();
      }
    }

    private Iterator outer;
    private Iterator<E> inner;
  }

  static class ExtendEntry<E> implements Map.Entry<Object, E> {
    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public E getValue() {
      return value;
    }

    @Override
    public E setValue(E value) {
      throw new UnsupportedOperationException();
    }

    ExtendEntry(Object key, E value) {
      this.key = key;
      this.value = value;
    }

    private Object key;
    private E value;
  }

  static class ExtendEntryIterator<E> extends IterableIterator<Map.Entry<Object, E>> {
    @Override
    public boolean hasNext() {
      return inner != null;
    }

    @Override
    public Map.Entry<Object, E> next() {
      ExtendEntry<E> curr = new ExtendEntry<E>(key, inner.next());
      if (!inner.hasNext()) {
        if (outer.hasNext()) {
          Map.Entry entry = (Map.Entry) outer.next();
          key = entry.getKey();
          inner = ((Iterable<E>) entry.getValue()).iterator();
        } else {
          inner = null;
        }
      }
      return curr;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    ExtendEntryIterator(IterableIterator<?> iterator) {
      outer = iterator;
      if (iterator.hasNext()) {
        Map.Entry entry = (Map.Entry) iterator.next();
        key = entry.getKey();
        inner = ((Iterable<E>) entry.getValue()).iterator();
      }
    }

    private Iterator outer;
    private Iterator<E> inner;
    private Object key;
  }

  class ExtendEntryStartFromIterator extends ExtendEntryIterator<T> {
    ExtendEntryStartFromIterator(int start, int order) {
      super(entryIterator(null, null, order));
      int skip = (order == ASCENT_ORDER) ? start : nElems - start - 1;
      while (--skip >= 0 && hasNext()) {
        next();
      }
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new ExtendIterator<T>(index.iterator());
  }

  @Override
  public IterableIterator<Map.Entry<Object, T>> entryIterator() {
    return new ExtendEntryIterator<T>(index.entryIterator());
  }

  @Override
  public IterableIterator<T> iterator(Key from, Key till, int order) {
    return new ExtendIterator<T>(index.iterator(from, till, order));
  }

  @Override
  public IterableIterator<T> iterator(Object from, Object till, int order) {
    return new ExtendIterator<T>(index.iterator(from, till, order));
  }

  @Override
  public IterableIterator<Map.Entry<Object, T>> entryIterator(Key from, Key till, int order) {
    return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
  }

  @Override
  public IterableIterator<Map.Entry<Object, T>> entryIterator(Object from, Object till, int order) {
    return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
  }

  @Override
  public IterableIterator<T> prefixIterator(String prefix) {
    return prefixIterator(prefix, ASCENT_ORDER);
  }

  @Override
  public IterableIterator<T> prefixIterator(String prefix, int order) {
    return new ExtendIterator<T>(index.prefixIterator(prefix, order));
  }

  @Override
  public Class getKeyType() {
    return index.getKeyType();
  }

  @Override
  public Class[] getKeyTypes() {
    return new Class[] {getKeyType()};
  }

  @Override
  public boolean put(Key key, T obj) {
    Object s = index.get(key);
    Storage storage = getStorage();
    int oid = storage.getOid(obj);
    if (oid == 0) {
      oid = storage.makePersistent(obj);
    }
    if (s == null) {
      Relation<T, ThickIndex> r = storage.<T, ThickIndex>createRelation(null);
      r.add(obj);
      index.put(key, r);
    } else if (s instanceof Relation) {
      Relation rel = (Relation) s;
      if (rel.size() == BTREE_THRESHOLD) {
        IPersistentSet<T> ps = storage.<T>createBag();
        for (int i = 0; i < BTREE_THRESHOLD; i++) {
          ps.add((T) rel.get(i));
        }
        Assert.that(ps.add(obj));
        index.set(key, ps);
        rel.deallocate();
      } else {
        int l = 0, n = rel.size(), r = n;
        while (l < r) {
          int m = (l + r) >>> 1;
          if (storage.getOid(rel.getRaw(m)) <= oid) {
            l = m + 1;
          } else {
            r = m;
          }
        }
        rel.insert(r, obj);
      }
    } else {
      Assert.that(((IPersistentSet<T>) s).add(obj));
    }
    nElems += 1;
    modify();
    return true;
  }

  @Override
  public T set(Key key, T obj) {
    Object s = index.get(key);
    Storage storage = getStorage();
    int oid = storage.getOid(obj);
    if (oid == 0) {
      oid = storage.makePersistent(obj);
    }
    if (s == null) {
      Relation<T, ThickIndex> r = storage.<T, ThickIndex>createRelation(null);
      r.add(obj);
      index.put(key, r);
      nElems += 1;
      modify();
      return null;
    } else if (s instanceof Relation) {
      Relation r = (Relation) s;
      if (r.size() == 1) {
        Object prev = r.get(0);
        r.set(0, obj);
        return (T) prev;
      }
    }
    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
  }

  @Override
  public boolean unlink(Key key, T obj) {
    return removeIfExists(key, obj);
  }

  boolean removeIfExists(Key key, T obj) {
    Object s = index.get(key);
    if (s instanceof Relation) {
      Relation rel = (Relation) s;
      Storage storage = getStorage();
      int oid = storage.getOid(obj);
      int l = 0, n = rel.size(), r = n;
      while (l < r) {
        int m = (l + r) >>> 1;
        if (storage.getOid(rel.getRaw(m)) < oid) {
          l = m + 1;
        } else {
          r = m;
        }
      }
      if (r < n && storage.getOid(rel.getRaw(r)) == oid) {
        rel.remove(r);
        if (rel.size() == 0) {
          index.remove(key, rel);
          rel.deallocate();
        }
        nElems -= 1;
        modify();
        return true;
      }
    } else if (s instanceof IPersistentSet) {
      IPersistentSet ps = (IPersistentSet) s;
      if (ps.remove(obj)) {
        if (ps.size() == 0) {
          index.remove(key, ps);
          ps.deallocate();
        }
        nElems -= 1;
        modify();
        return true;
      }
    }
    return false;
  }

  @Override
  public void remove(Key key, T obj) {
    if (!removeIfExists(key, obj)) {
      throw new StorageError(StorageError.KEY_NOT_FOUND);
    }
  }

  @Override
  public T remove(Key key) {
    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
  }

  @Override
  public boolean put(Object key, T obj) {
    return put(Btree.getKeyFromObject(key), obj);
  }

  @Override
  public T set(Object key, T obj) {
    return set(Btree.getKeyFromObject(key), obj);
  }

  @Override
  public void remove(Object key, T obj) {
    remove(Btree.getKeyFromObject(key), obj);
  }

  @Override
  public T remove(String key) {
    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
  }

  @Override
  public T removeKey(Object key) {
    throw new StorageError(StorageError.KEY_NOT_UNIQUE);
  }

  @Override
  public void deallocate() {
    clear();
    index.deallocate();
    super.deallocate();
  }

  @Override
  public int indexOf(Key key) {
    PersistentIterator iterator = (PersistentIterator) iterator(null, key, DESCENT_ORDER);
    int i;
    for (i = -1; iterator.nextOid() != 0; i++);
    return i;
  }

  @Override
  public T getAt(int i) {
    IterableIterator<Map.Entry<Object, T>> iterator;
    if (i < 0 || i >= nElems) {
      throw new IndexOutOfBoundsException("Position " + i + ", index size " + nElems);
    }
    if (i <= (nElems / 2)) {
      iterator = entryIterator(null, null, ASCENT_ORDER);
      while (--i >= 0) {
        iterator.next();
      }
    } else {
      iterator = entryIterator(null, null, DESCENT_ORDER);
      i -= nElems;
      while (++i < 0) {
        iterator.next();
      }
    }
    return iterator.next().getValue();
  }

  @Override
  public IterableIterator<Map.Entry<Object, T>> entryIterator(int start, int order) {
    return new ExtendEntryStartFromIterator(start, order);
  }

  @Override
  public boolean isUnique() {
    return false;
  }
}
