package org.garret.perst.impl;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.garret.perst.FieldIndex;
import org.garret.perst.IValue;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;
import org.garret.perst.Query;
import org.garret.perst.StorageError;

class RndBtreeMultiFieldIndex<T> extends RndBtree<T> implements FieldIndex<T> {
  String className;
  String[] fieldName;

  transient Class cls;
  transient Field[] fld;

  RndBtreeMultiFieldIndex() {}

  RndBtreeMultiFieldIndex(Class cls, String[] fieldName, boolean unique) {
    this.cls = cls;
    this.unique = unique;
    this.fieldName = fieldName;
    this.className = ClassDescriptor.getClassName(cls);
    locateFields();
    type = ClassDescriptor.tpValue;
  }

  private final void locateFields() {
    Class scope = cls;
    fld = new Field[fieldName.length];
    for (int i = 0; i < fieldName.length; i++) {
      fld[i] = ClassDescriptor.locateField(cls, fieldName[i]);
      if (fld[i] == null) {
        throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND,
            className + "." + fieldName[i]);
      }
    }
  }

  @Override
  public Class getIndexedClass() {
    return cls;
  }

  @Override
  public Field[] getKeyFields() {
    return fld;
  }

  @Override
  public void onLoad() {
    cls = ClassDescriptor.loadClass(getStorage(), className);
    locateFields();
  }

  static class CompoundKey implements Comparable, IValue {
    Object[] keys;

    @Override
    public int compareTo(Object o) {
      CompoundKey c = (CompoundKey) o;
      int n = keys.length < c.keys.length ? keys.length : c.keys.length;
      for (int i = 0; i < n; i++) {
        if (keys[i] != c.keys[i]) {
          if (keys[i] == null) {
            return -1;
          } else if (c.keys[i] == null) {
            return 1;
          } else {
            int diff = ((Comparable) keys[i]).compareTo(c.keys[i]);
            if (diff != 0) {
              return diff;
            }
          }
        }
      }
      return 0; // allow to compare part of the compound key
    }

    CompoundKey(Object[] keys) {
      this.keys = keys;
    }
  }

  private Key convertKey(Key key) {
    if (key == null) {
      return null;
    }
    if (key.type != ClassDescriptor.tpArrayOfObject) {
      throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
    }
    return new Key(new CompoundKey((Object[]) key.oval), key.inclusion != 0);
  }

  private Key extractKey(Object obj) {
    Object[] keys = new Object[fld.length];
    try {
      for (int i = 0; i < keys.length; i++) {
        Object val = fld[i].get(obj);
        keys[i] = val;
        if (!ClassDescriptor.isEmbedded(val)) {
          getStorage().makePersistent(val);
        }
      }
    } catch (Exception x) {
      throw new StorageError(StorageError.ACCESS_VIOLATION, x);
    }
    return new Key(new CompoundKey(keys));
  }

  @Override
  public boolean put(T obj) {
    return super.put(extractKey(obj), obj);
  }

  @Override
  public T set(T obj) {
    return super.set(extractKey(obj), obj);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    MultiFieldValue[] arr = new MultiFieldValue[c.size()];
    Iterator<? extends T> e = c.iterator();
    try {
      for (int i = 0; e.hasNext(); i++) {
        T obj = e.next();
        Comparable[] values = new Comparable[fld.length];
        for (int j = 0; j < values.length; j++) {
          values[j] = (Comparable) fld[j].get(obj);
        }
        arr[i] = new MultiFieldValue(obj, values);
      }
    } catch (Exception x) {
      throw new StorageError(StorageError.ACCESS_VIOLATION, x);
    }
    Arrays.sort(arr);
    for (int i = 0; i < arr.length; i++) {
      add((T) arr[i].obj);
    }
    return arr.length > 0;
  }

  @Override
  public boolean unlink(Key key, T obj) {
    return super.unlink(convertKey(key), obj);
  }

  @Override
  public boolean remove(Object obj) {
    return super.removeIfExists(extractKey(obj), obj);
  }

  @Override
  public T remove(Key key) {
    return super.remove(convertKey(key));
  }

  @Override
  public boolean containsObject(T obj) {
    Key key = extractKey(obj);
    if (unique) {
      return super.get(key) != null;
    } else {
      Object[] mbrs = get(key, key);
      for (int i = 0; i < mbrs.length; i++) {
        if (mbrs[i] == obj) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean contains(Object obj) {
    Key key = extractKey(obj);
    if (unique) {
      return super.get(key) != null;
    } else {
      Object[] mbrs = get(key, key);
      for (int i = 0; i < mbrs.length; i++) {
        if (mbrs[i].equals(obj)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void append(T obj) {
    throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE);
  }

  @Override
  public T[] get(Key from, Key till) {
    ArrayList list = new ArrayList();
    if (root != null) {
      root.find(convertKey(from), convertKey(till), height, list);
    }
    return (T[]) list.toArray((T[]) Array.newInstance(cls, list.size()));
  }

  @Override
  public T[] getPrefix(String prefix) {
    throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
  }


  @Override
  public T[] prefixSearch(String key) {
    throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
  }

  @Override
  public T[] toArray() {
    T[] arr = (T[]) Array.newInstance(cls, nElems);
    if (root != null) {
      root.traverseForward(height, arr, 0);
    }
    return arr;
  }

  @Override
  public T get(Key key) {
    return super.get(convertKey(key));
  }

  @Override
  public int indexOf(Key key) {
    return super.indexOf(convertKey(key));
  }

  @Override
  public IterableIterator<T> iterator(Key from, Key till, int order) {
    return super.iterator(convertKey(from), convertKey(till), order);
  }

  @Override
  public IterableIterator<Map.Entry<Object, T>> entryIterator(Key from, Key till, int order) {
    return super.entryIterator(convertKey(from), convertKey(till), order);
  }

  @Override
  public IterableIterator<T> queryByExample(T obj) {
    Key key = extractKey(obj);
    return iterator(key, key, ASCENT_ORDER);
  }

  @Override
  public IterableIterator<T> select(String predicate) {
    Query<T> query = new QueryImpl<T>(getStorage());
    return query.select(cls, iterator(), predicate);
  }

  @Override
  public boolean isCaseInsensitive() {
    return false;
  }
}


class RndBtreeCaseInsensitiveMultiFieldIndex<T> extends RndBtreeMultiFieldIndex<T> {
  RndBtreeCaseInsensitiveMultiFieldIndex() {}

  RndBtreeCaseInsensitiveMultiFieldIndex(Class cls, String[] fieldNames, boolean unique) {
    super(cls, fieldNames, unique);
  }

  @Override
  Key checkKey(Key key) {
    if (key != null) {
      CompoundKey ck = (CompoundKey) key.oval;
      for (int i = 0; i < ck.keys.length; i++) {
        if (ck.keys[i] instanceof String) {
          ck.keys[i] = ((String) ck.keys[i]).toLowerCase();
        }
      }
    }
    return super.checkKey(key);
  }

  @Override
  public boolean isCaseInsensitive() {
    return true;
  }
}
