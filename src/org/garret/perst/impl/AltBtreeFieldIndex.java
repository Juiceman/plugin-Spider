package org.garret.perst.impl;
import  org.garret.perst.*;

import java.lang.reflect.*;
import java.util.*;

class AltBtreeFieldIndex<T> extends AltBtree<T> implements FieldIndex<T> { 
    String className;
    String fieldName;
    long   autoincCount;
    transient Class cls;
    transient Field fld;

    AltBtreeFieldIndex() {}
    
    private final void locateField() 
    {
        fld = ClassDescriptor.locateField(cls, fieldName);
        if (fld == null) { 
           throw new StorageError(StorageError.INDEXED_FIELD_NOT_FOUND, className + "." + fieldName);
        }
    }

    @Override
    public Class getIndexedClass() { 
        return cls;
    }

    @Override
    public Field[] getKeyFields() { 
        return new Field[]{fld};
    }

    @Override
    public void onLoad()
    {
        cls = ClassDescriptor.loadClass(getStorage(), className);
        locateField();
    }

    AltBtreeFieldIndex(Class cls, String fieldName, boolean unique) {
        this.cls = cls;
        this.unique = unique;
        this.fieldName = fieldName;
        this.className = ClassDescriptor.getClassName(cls);
        locateField();
        type = checkType(fld.getType());
    }

    protected Key extractKey(Object obj) { 
        try { 
            Field f = fld;
            Key key = null;
            switch (type) {
              case ClassDescriptor.tpBoolean:
                key = new Key(f.getBoolean(obj));
                break;
              case ClassDescriptor.tpByte:
                key = new Key(f.getByte(obj));
                break;
              case ClassDescriptor.tpShort:
                key = new Key(f.getShort(obj));
                break;
              case ClassDescriptor.tpChar:
                key = new Key(f.getChar(obj));
                break;
              case ClassDescriptor.tpInt:
                key = new Key(f.getInt(obj));
                break;            
              case ClassDescriptor.tpObject:
                {
                    Object val = f.get(obj);
                    key = new Key(val, getStorage().makePersistent(val), true);
                    break;
                }
              case ClassDescriptor.tpLong:
                key = new Key(f.getLong(obj));
                break;            
              case ClassDescriptor.tpDate:
                key = new Key((Date)f.get(obj));
                break;
              case ClassDescriptor.tpFloat:
                key = new Key(f.getFloat(obj));
                break;
              case ClassDescriptor.tpDouble:
                key = new Key(f.getDouble(obj));
                break;
              case ClassDescriptor.tpEnum:
                key = new Key((Enum)f.get(obj));
                break;
              case ClassDescriptor.tpString:
                {
                    Object val = f.get(obj);
                    if (val != null) { 
                        key = new Key((String)val);
                    }
                }
                break;
              case ClassDescriptor.tpValue:
                key = new Key((IValue)f.get(obj));
                break;
              default:
                Assert.failed("Invalid type");
            }
            return key;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
            

    @Override
    public boolean put(T obj) {
        Key key = extractKey(obj);
        return key != null && super.insert(key, obj, false) == null;
    }

    @Override
    public T set(T obj) {
        Key key = extractKey(obj);
        if (key == null) {
            throw new StorageError(StorageError.KEY_IS_NULL);
        }
        return super.set(key, obj);
    }

    @Override
    public boolean add(T obj) {
        return put(obj);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        FieldValue[] arr = new FieldValue[c.size()];
        Iterator<? extends T> e = c.iterator();
        try { 
            for (int i = 0; e.hasNext(); i++) {
                T obj = e.next();
                arr[i] = new FieldValue(obj, fld.get(obj));
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        Arrays.sort(arr);
        for (int i = 0; i < arr.length; i++) {
            add((T)arr[i].obj);
        }
        return arr.length > 0;
    }

    @Override
    public boolean remove(Object obj) {
        Key key = extractKey(obj);
        return key != null && super.removeIfExists(key, obj);
    }

    @Override
    public boolean containsObject(T obj) {
        Key key = extractKey(obj);
        if (key == null) { 
            return false;
        }
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
        if (key == null) { 
            return false;
        }
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
    public synchronized void append(T obj) {
        Key key;
        try { 
            switch (type) {
              case ClassDescriptor.tpInt:
                key = new Key((int)autoincCount);
                fld.setInt(obj, (int)autoincCount);
                break;            
              case ClassDescriptor.tpLong:
                key = new Key(autoincCount);
                fld.setLong(obj, autoincCount);
                break;            
              default:
                throw new StorageError(StorageError.UNSUPPORTED_INDEX_TYPE, fld.getType());
            }
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
        autoincCount += 1;
        getStorage().modify(obj);
        super.insert(key, obj, false);
    }

    @Override
    public T[] getPrefix(String prefix) { 
        ArrayList<T> list = getList(new Key(prefix, true), new Key(prefix + Character.MAX_VALUE, false));
        return list.toArray((T[])Array.newInstance(cls, list.size()));        
    }

    @Override
    public T[] prefixSearch(String key) { 
        ArrayList<T> list = prefixSearchList(key);
        return list.toArray((T[])Array.newInstance(cls, list.size()));
    }

    @Override
    public T[] get(Key from, Key till) {
        ArrayList<T> list = new ArrayList();
        if (root != null) { 
            root.find(checkKey(from), checkKey(till), height, list);
        }
        return list.toArray((T[])Array.newInstance(cls, list.size()));
    }

    @Override
    public T[] toArray() {
        T[] arr = (T[])Array.newInstance(cls, nElems);
        if (root != null) { 
            root.traverseForward(height, arr, 0);
        }
        return arr;
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

class AltBtreeCaseInsensitiveFieldIndex<T> extends AltBtreeFieldIndex<T> {    
    AltBtreeCaseInsensitiveFieldIndex() {}

    AltBtreeCaseInsensitiveFieldIndex(Class cls, String fieldName, boolean unique) {
        super(cls, fieldName, unique);
    }

    @Override
    Key checkKey(Key key) { 
        if (key != null && key.oval instanceof String) { 
            key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
        }
        return super.checkKey(key);
    }  

    @Override
    public boolean isCaseInsensitive() { 
        return true;
    }
}