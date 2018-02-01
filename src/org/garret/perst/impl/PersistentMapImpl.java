package org.garret.perst.impl;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import org.garret.perst.GenericIndex;
import org.garret.perst.IPersistent;
import org.garret.perst.IPersistentMap;
import org.garret.perst.IValue;
import org.garret.perst.Index;
import org.garret.perst.Key;
import org.garret.perst.Link;
import org.garret.perst.Persistent;
import org.garret.perst.PersistentComparator;
import org.garret.perst.PersistentResource;
import org.garret.perst.Query;
import org.garret.perst.SortedCollection;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

class PersistentMapImpl<K extends Comparable, V> extends PersistentResource
    implements IPersistentMap<K, V> {
  IPersistent index;
  Object keys;
  Link<V> values;
  int type;

  transient volatile Set<Entry<K, V>> entrySet;
  transient volatile Set<K> keySet;
  transient volatile Collection<V> valuesCol;

  static final int BTREE_TRESHOLD = 128;

  PersistentMapImpl(Storage storage, Class keyType, int initialSize) {
    super(storage);
    type = getTypeCode(keyType);
    keys = new Comparable[initialSize];
    values = storage.<V>createLink(initialSize);
  }

  static class PersistentMapEntry<K extends Comparable, V> extends Persistent
      implements Entry<K, V> {
    K key;
    V value;

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      modify();
      V prevValue = this.value;
      this.value = value;
      return prevValue;
    }

    PersistentMapEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    PersistentMapEntry() {}
  }

  static class PersistentMapComparator<K extends Comparable, V>
      extends PersistentComparator<PersistentMapEntry<K, V>> {
    @Override
    public int compareMembers(PersistentMapEntry<K, V> m1, PersistentMapEntry<K, V> m2) {
      return m1.key.compareTo(m2.key);
    }

    @Override
    public int compareMemberWithKey(PersistentMapEntry<K, V> mbr, Object key) {
      return mbr.key.compareTo(key);
    }
  }

  PersistentMapImpl() {}

  protected int getTypeCode(Class c) {
    if (c.equals(byte.class) || c.equals(Byte.class)) {
      return ClassDescriptor.tpByte;
    } else if (c.equals(short.class) || c.equals(Short.class)) {
      return ClassDescriptor.tpShort;
    } else if (c.equals(char.class) || c.equals(Character.class)) {
      return ClassDescriptor.tpChar;
    } else if (c.equals(int.class) || c.equals(Integer.class)) {
      return ClassDescriptor.tpInt;
    } else if (c.equals(long.class) || c.equals(Long.class)) {
      return ClassDescriptor.tpLong;
    } else if (c.equals(float.class) || c.equals(Float.class)) {
      return ClassDescriptor.tpFloat;
    } else if (c.equals(double.class) || c.equals(Double.class)) {
      return ClassDescriptor.tpDouble;
    } else if (c.equals(String.class)) {
      return ClassDescriptor.tpString;
    } else if (c.equals(boolean.class) || c.equals(Boolean.class)) {
      return ClassDescriptor.tpBoolean;
    } else if (c.isEnum()) {
      return ClassDescriptor.tpEnum;
    } else if (c.equals(java.util.Date.class)) {
      return ClassDescriptor.tpDate;
    } else if (IValue.class.isAssignableFrom(c)) {
      return ClassDescriptor.tpValue;
    } else {
      return ClassDescriptor.tpObject;
    }
  }

  @Override
  public int size() {
    return index != null ? ((Collection) index).size() : values.size();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsValue(Object value) {
    Iterator<Entry<K, V>> i = entrySet().iterator();
    if (value == null) {
      while (i.hasNext()) {
        Entry<K, V> e = i.next();
        if (e.getValue() == null)
          return true;
      }
    } else {
      while (i.hasNext()) {
        Entry<K, V> e = i.next();
        if (value.equals(e.getValue()))
          return true;
      }
    }
    return false;
  }

  private int binarySearch(Object key) {
    Comparable[] keys = (Comparable[]) this.keys;
    int l = 0, r = values.size();
    while (l < r) {
      int i = (l + r) >> 1;
      if (keys[i].compareTo(key) < 0) {
        l = i + 1;
      } else {
        r = i;
      }
    }
    return r;
  }

  @Override
  public boolean containsKey(Object key) {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        return ((SortedCollection) index).containsKey(key);
      } else {
        Key k = generateKey(key);
        return ((Index) index).entryIterator(k, k, GenericIndex.ASCENT_ORDER).hasNext();
      }
    } else {
      int i = binarySearch(key);
      return i < values.size() && ((Comparable[]) keys)[i].equals(key);
    }
  }

  @Override
  public V get(Object key) {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        PersistentMapEntry<K, V> entry =
            ((SortedCollection<PersistentMapEntry<K, V>>) index).get(key);
        return (entry != null) ? entry.value : null;
      } else {
        return ((Index<V>) index).get(generateKey(key));
      }
    } else {
      int i = binarySearch(key);
      if (i < values.size() && ((Comparable[]) keys)[i].equals(key)) {
        return values.get(i);
      }
      return null;
    }
  }

  @Override
  public Entry<K, V> getEntry(Object key) {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        return ((SortedCollection<PersistentMapEntry<K, V>>) index).get(key);
      } else {
        V value = ((Index<V>) index).get(generateKey(key));
        return value != null ? new PersistentMapEntry((K) key, value) : null;
      }
    } else {
      int i = binarySearch(key);
      if (i < values.size() && ((Comparable[]) keys)[i].equals(key)) {
        V value = values.get(i);
        return value != null ? new PersistentMapEntry((K) key, value) : null;
      }
      return null;
    }
  }

  @Override
  public V put(K key, V value) {
    V prev = null;
    if (index == null) {
      int size = values.size();
      int i = binarySearch(key);
      if (i < size && key.equals(((Comparable[]) keys)[i])) {
        prev = values.set(i, value);
      } else {
        if (size == BTREE_TRESHOLD) {
          Comparable[] keys = (Comparable[]) this.keys;
          if (type == ClassDescriptor.tpValue) {
            SortedCollection<PersistentMapEntry<K, V>> col =
                getStorage().<PersistentMapEntry<K, V>>createSortedCollection(
                    new PersistentMapComparator<K, V>(), true);
            index = col;
            for (i = 0; i < size; i++) {
              col.add(new PersistentMapEntry(keys[i], values.get(i)));
            }
            prev = insertInSortedCollection(key, value);
          } else {
            Index<V> idx = getStorage().<V>createIndex(Btree.mapKeyType(type), true);
            index = idx;
            for (i = 0; i < size; i++) {
              idx.set(generateKey(keys[i]), values.get(i));
            }
            prev = idx.set(generateKey(key), value);
          }
          this.keys = null;
          this.values = null;
          modify();
        } else {
          Object[] oldKeys = (Object[]) keys;
          if (size >= oldKeys.length) {
            Comparable[] newKeys =
                new Comparable[size + 1 > oldKeys.length * 2 ? size + 1 : oldKeys.length * 2];
            System.arraycopy(oldKeys, 0, newKeys, 0, i);
            System.arraycopy(oldKeys, i, newKeys, i + 1, size - i);
            keys = newKeys;
            newKeys[i] = key;
          } else {
            System.arraycopy(oldKeys, i, oldKeys, i + 1, size - i);
            oldKeys[i] = key;
          }
          values.insert(i, value);
        }
      }
    } else {
      if (type == ClassDescriptor.tpValue) {
        prev = insertInSortedCollection(key, value);
      } else {
        prev = ((Index<V>) index).set(generateKey(key), value);
      }
    }
    return prev;
  }

  private V insertInSortedCollection(K key, V value) {
    SortedCollection<PersistentMapEntry<K, V>> col =
        (SortedCollection<PersistentMapEntry<K, V>>) index;
    PersistentMapEntry<K, V> entry = col.get(key);
    V prev = null;
    getStorage().makePersistent(value);
    if (entry == null) {
      col.add(new PersistentMapEntry(key, value));
    } else {
      prev = entry.setValue(value);
    }
    return prev;
  }

  @Override
  public V remove(Object key) {
    if (index == null) {
      int size = values.size();
      int i = binarySearch(key);
      if (i < size && ((Comparable[]) keys)[i].equals(key)) {
        System.arraycopy(keys, i + 1, keys, i, size - i - 1);
        ((Comparable[]) keys)[size - 1] = null;
        return values.remove(i);
      }
      return null;
    } else {
      if (type == ClassDescriptor.tpValue) {
        SortedCollection<PersistentMapEntry<K, V>> col =
            (SortedCollection<PersistentMapEntry<K, V>>) index;
        PersistentMapEntry<K, V> entry = col.get(key);
        if (entry == null) {
          return null;
        }
        col.remove(entry);
        return entry.value;
      } else {
        try {
          return ((Index<V>) index).remove(generateKey(key));
        } catch (StorageError x) {
          if (x.getErrorCode() == StorageError.KEY_NOT_FOUND) {
            return null;
          }
          throw x;
        }
      }
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> t) {
    Iterator<? extends Entry<? extends K, ? extends V>> i = t.entrySet().iterator();
    while (i.hasNext()) {
      Entry<? extends K, ? extends V> e = i.next();
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public void clear() {
    if (index != null) {
      ((Collection) index).clear();
    } else {
      values.clear();
      keys = new Comparable[((Comparable[]) keys).length];
    }
  }

  @Override
  public Set<K> keySet() {
    if (keySet == null) {
      keySet = new AbstractSet<K>() {
        @Override
        public Iterator<K> iterator() {
          return new Iterator<K>() {
            private Iterator<Entry<K, V>> i = entrySet().iterator();

            @Override
            public boolean hasNext() {
              return i.hasNext();
            }

            @Override
            public K next() {
              return i.next().getKey();
            }

            @Override
            public void remove() {
              i.remove();
            }
          };
        }

        @Override
        public int size() {
          return PersistentMapImpl.this.size();
        }

        @Override
        public boolean contains(Object k) {
          return PersistentMapImpl.this.containsKey(k);
        }
      };
    }
    return keySet;
  }

  @Override
  public Collection<V> values() {
    if (valuesCol == null) {
      valuesCol = new AbstractCollection<V>() {
        @Override
        public Iterator<V> iterator() {
          return new Iterator<V>() {
            private Iterator<Entry<K, V>> i = entrySet().iterator();

            @Override
            public boolean hasNext() {
              return i.hasNext();
            }

            @Override
            public V next() {
              return i.next().getValue();
            }

            @Override
            public void remove() {
              i.remove();
            }
          };
        }

        @Override
        public int size() {
          return PersistentMapImpl.this.size();
        }

        @Override
        public boolean contains(Object v) {
          return PersistentMapImpl.this.containsValue(v);
        }
      };
    }
    return valuesCol;
  }

  protected Iterator<Entry<K, V>> entryIterator() {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        return new Iterator<Entry<K, V>>() {
          private Iterator<PersistentMapEntry<K, V>> i =
              ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator();

          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Entry<K, V> next() {
            return i.next();
          }

          @Override
          public void remove() {
            i.remove();
          }
        };
      } else {
        return new Iterator<Entry<K, V>>() {
          private Iterator<Entry<Object, V>> i = ((Index<V>) index).entryIterator();

          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Entry<K, V> next() {
            final Entry<Object, V> e = i.next();
            return new Entry<K, V>() {
              @Override
              public K getKey() {
                return (K) e.getKey();
              }

              @Override
              public V getValue() {
                return e.getValue();
              }

              @Override
              public V setValue(V value) {
                throw new UnsupportedOperationException("Entry.Map.setValue");
              }
            };
          }

          @Override
          public void remove() {
            i.remove();
          }
        };
      }
    } else {
      return new Iterator<Entry<K, V>>() {
        private int i = -1;

        @Override
        public boolean hasNext() {
          return i + 1 < values.size();
        }

        @Override
        public Entry<K, V> next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          i += 1;
          return new Entry<K, V>() {
            @Override
            public K getKey() {
              return (K) (((Comparable[]) keys)[i]);
            }

            @Override
            public V getValue() {
              return values.get(i);
            }

            @Override
            public V setValue(V value) {
              throw new UnsupportedOperationException("Entry.Map.setValue");
            }
          };
        }

        @Override
        public void remove() {
          if (i < 0) {
            throw new IllegalStateException();
          }
          int size = values.size();
          System.arraycopy(keys, i + 1, keys, i, size - i - 1);
          ((Comparable[]) keys)[size - 1] = null;
          values.removeObject(i);
          i -= 1;
        }
      };
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    if (entrySet == null) {
      entrySet = new AbstractSet<Entry<K, V>>() {
        @Override
        public Iterator<Entry<K, V>> iterator() {
          return entryIterator();
        }

        @Override
        public int size() {
          return PersistentMapImpl.this.size();
        }

        @Override
        public boolean remove(Object o) {
          if (!(o instanceof Map.Entry)) {
            return false;
          }
          Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
          K key = entry.getKey();
          V value = entry.getValue();
          if (value != null) {
            V v = PersistentMapImpl.this.get(key);
            if (value.equals(v)) {
              PersistentMapImpl.this.remove(key);
              return true;
            }
          } else {
            if (PersistentMapImpl.this.containsKey(key)
                && PersistentMapImpl.this.get(key) == null) {
              PersistentMapImpl.this.remove(key);
              return true;
            }
          }
          return false;
        }

        @Override
        public boolean contains(Object k) {
          Entry<K, V> e = (Entry<K, V>) k;
          if (e.getValue() != null) {
            return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
          } else {
            return PersistentMapImpl.this.containsKey(e.getKey())
                && PersistentMapImpl.this.get(e.getKey()) == null;
          }
        }
      };
    }
    return entrySet;
  }


  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Map)) {
      return false;
    }
    Map<K, V> t = (Map<K, V>) o;
    if (t.size() != size()) {
      return false;
    }

    try {
      Iterator<Entry<K, V>> i = entrySet().iterator();
      while (i.hasNext()) {
        Entry<K, V> e = i.next();
        K key = e.getKey();
        V value = e.getValue();
        if (value == null) {
          if (!(t.get(key) == null && t.containsKey(key))) {
            return false;
          }
        } else {
          if (!value.equals(t.get(key))) {
            return false;
          }
        }
      }
    } catch (ClassCastException unused) {
      return false;
    } catch (NullPointerException unused) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int h = 0;
    Iterator<Entry<K, V>> i = entrySet().iterator();
    while (i.hasNext()) {
      h += i.next().hashCode();
    }
    return h;
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("{");

    Iterator<Entry<K, V>> i = entrySet().iterator();
    boolean hasNext = i.hasNext();
    while (hasNext) {
      Entry<K, V> e = i.next();
      K key = e.getKey();
      V value = e.getValue();
      if (key == this) {
        buf.append("(this Map)");
      } else {
        buf.append(key);
      }
      buf.append("=");
      if (value == this) {
        buf.append("(this Map)");
      } else {
        buf.append(value);
      }
      hasNext = i.hasNext();
      if (hasNext) {
        buf.append(", ");
      }
    }

    buf.append("}");
    return buf.toString();
  }

  final Key generateKey(Object key) {
    return generateKey(key, true);
  }

  final Key generateKey(Object key, boolean inclusive) {
    if (key instanceof Integer) {
      return new Key(((Integer) key).intValue(), inclusive);
    } else if (key instanceof Byte) {
      return new Key(((Byte) key).byteValue(), inclusive);
    } else if (key instanceof Character) {
      return new Key(((Character) key).charValue(), inclusive);
    } else if (key instanceof Short) {
      return new Key(((Short) key).shortValue(), inclusive);
    } else if (key instanceof Long) {
      return new Key(((Long) key).longValue(), inclusive);
    } else if (key instanceof Float) {
      return new Key(((Float) key).floatValue(), inclusive);
    } else if (key instanceof Double) {
      return new Key(((Double) key).doubleValue(), inclusive);
    } else if (key instanceof String) {
      return new Key((String) key, inclusive);
    } else if (key instanceof Enum) {
      return new Key((Enum) key, inclusive);
    } else if (key instanceof java.util.Date) {
      return new Key((java.util.Date) key, inclusive);
    } else if (key instanceof IValue) {
      return new Key((IValue) key, inclusive);
    } else {
      return new Key(key, inclusive);
    }
  }

  @Override
  public Comparator<? super K> comparator() {
    return null;
  }

  @Override
  public SortedMap<K, V> subMap(K from, K to) {
    if (from.compareTo(to) > 0) {
      throw new IllegalArgumentException("from > to");
    }
    return new SubMap(from, to);
  }

  @Override
  public SortedMap<K, V> headMap(K to) {
    return new SubMap(null, to);
  }

  @Override
  public SortedMap<K, V> tailMap(K from) {
    return new SubMap(from, null);
  }

  private class SubMap extends AbstractMap<K, V> implements SortedMap<K, V> {
    private Key fromKey;
    private Key toKey;
    private K from;
    private K to;
    volatile Set<Entry<K, V>> entrySet;

    SubMap(K from, K to) {
      this.from = from;
      this.to = to;
      this.fromKey = from != null ? generateKey(from, true) : null;
      this.toKey = to != null ? generateKey(to, false) : null;
    }

    @Override
    public boolean isEmpty() {
      return entrySet().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return inRange((K) key) && PersistentMapImpl.this.containsKey(key);
    }

    @Override
    public V get(Object key) {
      if (!inRange((K) key)) {
        return null;
      }
      return PersistentMapImpl.this.get(key);
    }

    @Override
    public V put(K key, V value) {
      if (!inRange(key)) {
        throw new IllegalArgumentException("key out of range");
      }
      return PersistentMapImpl.this.put(key, value);
    }

    @Override
    public Comparator<? super K> comparator() {
      return null;
    }

    @Override
    public K firstKey() {
      return entryIterator(GenericIndex.ASCENT_ORDER).next().getKey();
    }

    @Override
    public K lastKey() {
      return entryIterator(GenericIndex.DESCENT_ORDER).next().getKey();
    }

    protected Iterator<Entry<K, V>> entryIterator(final int order) {
      if (index != null) {
        if (type == ClassDescriptor.tpValue) {
          if (order == GenericIndex.ASCENT_ORDER) {
            return new Iterator<Entry<K, V>>() {
              private Iterator<PersistentMapEntry<K, V>> i =
                  ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator(fromKey, toKey);

              @Override
              public boolean hasNext() {
                return i.hasNext();
              }

              @Override
              public Entry<K, V> next() {
                return i.next();
              }

              @Override
              public void remove() {
                i.remove();
              }
            };
          } else {
            return new Iterator<Entry<K, V>>() {
              private ArrayList<PersistentMapEntry<K, V>> entries =
                  ((SortedCollection<PersistentMapEntry<K, V>>) index).getList(fromKey, toKey);
              private int i = entries.size();

              @Override
              public boolean hasNext() {
                return i > 0;
              }

              @Override
              public Entry<K, V> next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                return entries.get(--i);
              }

              @Override
              public void remove() {
                if (i < entries.size() || entries.get(i) == null) {
                  throw new IllegalStateException();
                }
                ((SortedCollection<PersistentMapEntry<K, V>>) index).remove(entries.get(i));
                entries.set(i, null);
              }
            };
          }
        } else {
          return new Iterator<Entry<K, V>>() {
            private Iterator<Entry<Object, V>> i =
                ((Index<V>) index).entryIterator(fromKey, toKey, order);

            @Override
            public boolean hasNext() {
              return i.hasNext();
            }

            @Override
            public Entry<K, V> next() {
              final Entry<Object, V> e = i.next();
              return new Entry<K, V>() {
                @Override
                public K getKey() {
                  return (K) e.getKey();
                }

                @Override
                public V getValue() {
                  return e.getValue();
                }

                @Override
                public V setValue(V value) {
                  throw new UnsupportedOperationException("Entry.Map.setValue");
                }
              };
            }

            @Override
            public void remove() {
              i.remove();
            }
          };
        }
      } else {
        if (order == GenericIndex.ASCENT_ORDER) {
          final int beg = (from != null ? binarySearch(from) : 0) - 1;
          final int end = values.size();

          return new Iterator<Entry<K, V>>() {
            private int i = beg;

            @Override
            public boolean hasNext() {
              return i + 1 < end && (to == null || ((Comparable[]) keys)[i + 1].compareTo(to) < 0);
            }

            @Override
            public Entry<K, V> next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              i += 1;
              return new Entry<K, V>() {
                @Override
                public K getKey() {
                  return (K) ((Comparable[]) keys)[i];
                }

                @Override
                public V getValue() {
                  return values.get(i);
                }

                @Override
                public V setValue(V value) {
                  throw new UnsupportedOperationException("Entry.Map.setValue");
                }
              };
            }

            @Override
            public void remove() {
              if (i < 0) {
                throw new IllegalStateException();
              }
              int size = values.size();
              System.arraycopy(keys, i + 1, keys, i, size - i - 1);
              ((Comparable[]) keys)[size - 1] = null;
              values.removeObject(i);
              i -= 1;
            }
          };
        } else {
          final int beg = (to != null ? binarySearch(to) : 0) - 1;

          return new Iterator<Entry<K, V>>() {
            private int i = beg;

            @Override
            public boolean hasNext() {
              return i > 0 && (from == null || ((Comparable[]) keys)[i - 1].compareTo(from) >= 0);
            }

            @Override
            public Entry<K, V> next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              i -= 1;
              return new Entry<K, V>() {
                @Override
                public K getKey() {
                  return (K) ((Comparable[]) keys)[i];
                }

                @Override
                public V getValue() {
                  return values.get(i);
                }

                @Override
                public V setValue(V value) {
                  throw new UnsupportedOperationException("Entry.Map.setValue");
                }
              };
            }

            @Override
            public void remove() {
              if (i < 0) {
                throw new IllegalStateException();
              }
              int size = values.size();
              System.arraycopy(keys, i + 1, keys, i, size - i - 1);
              ((Comparable[]) keys)[size - 1] = null;
              values.removeObject(i);
            }
          };
        }
      }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      if (entrySet == null) {
        entrySet = new AbstractSet<Entry<K, V>>() {
          @Override
          public Iterator<Entry<K, V>> iterator() {
            return entryIterator(GenericIndex.ASCENT_ORDER);
          }

          @Override
          public int size() {
            Iterator<Entry<K, V>> i = iterator();
            int n;
            for (n = 0; i.hasNext(); i.next()) {
              n += 1;
            }
            return n;
          }

          @Override
          public boolean isEmpty() {
            return !iterator().hasNext();
          }

          @Override
          public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
              return false;
            }
            Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
            K key = entry.getKey();
            if (!inRange(key)) {
              return false;
            }
            V value = entry.getValue();
            if (value != null) {
              V v = PersistentMapImpl.this.get(key);
              if (value.equals(v)) {
                PersistentMapImpl.this.remove(key);
                return true;
              }
            } else {
              if (PersistentMapImpl.this.containsKey(key)
                  && PersistentMapImpl.this.get(key) == null) {
                PersistentMapImpl.this.remove(key);
                return true;
              }
            }
            return false;
          }

          @Override
          public boolean contains(Object k) {
            Entry<K, V> e = (Entry<K, V>) k;
            if (!inRange(e.getKey())) {
              return false;
            }
            if (e.getValue() != null) {
              return e.getValue().equals(PersistentMapImpl.this.get(e.getKey()));
            } else {
              return PersistentMapImpl.this.containsKey(e.getKey())
                  && PersistentMapImpl.this.get(e.getKey()) == null;
            }
          }
        };
      }
      return entrySet;
    }

    @Override
    public SortedMap<K, V> subMap(K from, K to) {
      if (!inRange2(from)) {
        throw new IllegalArgumentException("'from' out of range");
      }
      if (!inRange2(to)) {
        throw new IllegalArgumentException("'to' out of range");
      }
      return new SubMap(from, to);
    }

    @Override
    public SortedMap<K, V> headMap(K to) {
      if (!inRange2(to)) {
        throw new IllegalArgumentException("'to' out of range");
      }
      return new SubMap(this.from, to);
    }

    @Override
    public SortedMap<K, V> tailMap(K from) {
      if (!inRange2(from)) {
        throw new IllegalArgumentException("'from' out of range");
      }
      return new SubMap(from, this.to);
    }

    private boolean inRange(K key) {
      return (from == null || key.compareTo(from) >= 0) && (to == null || key.compareTo(to) < 0);
    }

    // This form allows the high endpoint (as well as all legit keys)
    private boolean inRange2(K key) {
      return (from == null || key.compareTo(from) >= 0) && (to == null || key.compareTo(to) <= 0);
    }
  }

  @Override
  public K firstKey() {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        return ((SortedCollection<PersistentMapEntry<K, V>>) index).iterator().next().key;
      } else {
        return (K) ((Index<V>) index).entryIterator().next().getKey();
      }
    } else {
      Comparable[] keys = (Comparable[]) this.keys;
      if (values.size() == 0) {
        throw new NoSuchElementException();
      }
      return (K) keys[0];
    }
  }

  @Override
  public K lastKey() {
    if (index != null) {
      if (type == ClassDescriptor.tpValue) {
        ArrayList<PersistentMapEntry<K, V>> entries =
            ((SortedCollection<PersistentMapEntry<K, V>>) index).getList(null, null);
        return entries.get(entries.size() - 1).key;
      } else {
        return (K) ((Index<V>) index).entryIterator(null, null, GenericIndex.DESCENT_ORDER).next()
            .getKey();
      }
    } else {
      int size = values.size();
      if (size == 0) {
        throw new NoSuchElementException();
      }
      return (K) ((Comparable[]) keys)[size - 1];
    }
  }

  @Override
  public Iterator<V> select(Class cls, String predicate) {
    Query<V> query = new QueryImpl<V>(getStorage());
    return query.select(cls, values().iterator(), predicate);
  }
}
