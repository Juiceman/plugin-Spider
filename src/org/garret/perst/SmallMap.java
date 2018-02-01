package org.garret.perst;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This class provide small embedded map (collection of &lt;key,value&gt; pairs).
 * Pairs are stored in the array in the order of their insertion.
 * Consequently operations with this map has linear ccomplexity.
 */ 
public class SmallMap<K,V> extends PersistentResource implements Map<K,V>
{
    private Pair<K,V>[] pairs;

    public SmallMap() {
        pairs = new Pair[0];
    }

    @Override
    public int size() { 
        return pairs.length;
    }

    @Override
    public boolean isEmpty() {
        return pairs.length == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return getEntry(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < pairs.length; i++) { 
            if (pairs[i].value == value || (value != null && value.equals(pairs[i].value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        Entry<K,V> entry = getEntry(key);
        return entry != null ? entry.getValue() : null;
    }
    
    public Entry<K,V> getEntry(Object key) {        
        for (int i = 0; i < pairs.length; i++) { 
            if (pairs[i].key.equals(key)) {
                return pairs[i];
            }
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        int n = pairs.length;
        if (key == null) { 
            throw new IllegalArgumentException("Key is null");
        }
        for (int i = 0; i < n; i++) { 
            if (key.equals(pairs[i].key)) {
                V oldValue = pairs[i].value;
                pairs[i].value = value;
                modify();
                return oldValue;
            }
        }
        Pair<K,V>[] newPairs = new Pair[n+1];
        System.arraycopy(pairs, 0, newPairs, 0, n);
        newPairs[n] = new Pair<K,V>(key, value);
        pairs = newPairs;
        modify();
        return null;
    }

    @Override
    public V remove(Object key) {
        Entry<K,V> e = removeEntry(key);
        return (e == null ? null : e.getValue());
    }

    public Entry<K,V> removeAt(int i) {
        Pair<K,V> pair = pairs[i];
        Pair<K,V>[] newPairs = new Pair[pairs.length-1];
        System.arraycopy(pairs, 0, newPairs, 0, i);
        System.arraycopy(pairs, i+1, newPairs, i, pairs.length-i-1);
        pairs = newPairs;
        modify();
        return pair;
    }

    final Entry<K,V> removeEntry(Object key) {
        for (int i = 0; i < pairs.length; i++) { 
            if (key.equals(pairs[i].key)) {
                return removeAt(i);
            }
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
       for (Iterator<? extends Map.Entry<? extends K, ? extends V>> i = m.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }
    
    @Override
    public void clear() { 
        pairs = new Pair[0];
        modify();
    }
    
    @Override
    public Set<K> keySet() {
        Set<K> ks = keySet;
        return (ks != null ? ks : (keySet = new KeySet()));
    }

    private final class KeySet extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new KeyIterator();
        }
        @Override
        public int size() {
            return pairs.length;
        }
        @Override
        public boolean contains(Object o) {
            return containsKey(o);
        }
        @Override
        public boolean remove(Object o) {
            return SmallMap.this.removeEntry(o) != null;
        }
        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

    @Override
    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null ? vs : (values = new Values()));
    }

    private final class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIterator();
        }
        @Override
        public int size() {
            return pairs.length;
        }
        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }
        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

    @Override
    public Set<Map.Entry<K,V>> entrySet() {
        Set<Map.Entry<K,V>> es = entrySet;
        return es != null ? es : (entrySet = new EntrySet());
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K,V>> {
        @Override
        public Iterator<Map.Entry<K,V>> iterator() {
            return new EntryIterator();
        }
        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<K,V> e = (Map.Entry<K,V>) o;
            Entry<K,V> candidate = getEntry(e.getKey());
            return candidate != null && candidate.equals(e);
        }
        @Override
        public boolean remove(Object o) {
            Entry<K,V> pair = getEntry(o);
            if (pair != null && pair.equals(o)) {
                removeEntry(o);
                return true;
            }
            return false;
        }
        @Override
        public int size() {
            return pairs.length;
        }
        @Override
        public void clear() {
            SmallMap.this.clear();
        }
    }

    static class Pair<K,V> implements Map.Entry<K,V>, IValue {
        K key;
        V value;

        Pair() {}

        Pair(K k, V v) {
            value = v;
            key = k;
        }

        @Override
        public final K getKey() {
            return key;
        }

        @Override
        public final V getValue() {
            return value;
        }

        /**
         * In case of updating pair value usingg this method it is necessary to explicitely call
         * modify() method for the parent SmallMap object
         */
        @Override
        public final V setValue(V newValue) {
	    V oldValue = value;
            value = newValue;
            return oldValue;
        }

        @Override
        public final boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) { 
                return false;
            }
            Map.Entry e = (Map.Entry)o;
            Object k1 = getKey();
            Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public final int hashCode() {
            return key.hashCode() ^ (value==null ? 0 : value.hashCode());
        }

        @Override
        public final String toString() {
            return getKey() + "=" + getValue();
        }
    }
    
    private abstract class ArrayIterator<E> implements Iterator<E> {
        int current;

        ArrayIterator() {
            current = -1;
        }

        @Override
        public final boolean hasNext() {
            return current+1 < pairs.length;
        }

        final Entry<K,V> nextEntry() {
            if (current+1 >= pairs.length) {
                throw new NoSuchElementException();
            }
            return pairs[++current];
        }
        
        @Override
        public void remove() {
            if (current < 0 || current >= pairs.length) {
                throw new IllegalStateException();
            }
            SmallMap.this.removeAt(current--);
        }
    }

    private final class ValueIterator extends ArrayIterator<V> {
        @Override
        public V next() {
            return nextEntry().getValue();
        }
    }

    private final class KeyIterator extends ArrayIterator<K> {
        @Override
        public K next() {
            return nextEntry().getKey();
        }
    }

    private final class EntryIterator extends ArrayIterator<Map.Entry<K,V>> {
        @Override
        public Map.Entry<K,V> next() {
            return nextEntry();
        }
    }

    @Override
    public boolean equals(Object o) {
	if (o == this) {
	    return true;
        }
	if (!(o instanceof Map)) {
	    return false;
        }
	Map<K,V> m = (Map<K,V>) o;
	if (m.size() != size()) {
	    return false;
        }
        Iterator<Entry<K,V>> i = entrySet().iterator();
        while (i.hasNext()) {
            Entry<K,V> e = i.next();
            K key = e.getKey();
            V value = e.getValue();
            if (value == null) {
                if (!(m.get(key) == null && m.containsKey(key))) {
                    return false;
                }
            } else {
                if (!value.equals(m.get(key))) {
                    return false;
                }
            }
        }
	return true;
    }

    @Override
    public int hashCode() {
	int h = 0;
	Iterator<Entry<K,V>> i = entrySet().iterator();
	while (i.hasNext()) {
	    h += i.next().hashCode();
        }
	return h;
    }

    @Override
    public String toString() {
	Iterator<Entry<K,V>> i = entrySet().iterator();
	if (! i.hasNext()) {
	    return "{}";
        }
	StringBuilder sb = new StringBuilder();
	sb.append('{');
	while (true) {
	    Entry<K,V> e = i.next();
	    K key = e.getKey();
	    V value = e.getValue();
	    sb.append(key   == this ? "(this Map)" : key);
	    sb.append('=');
	    sb.append(value == this ? "(this Map)" : value);
	    if (!i.hasNext()) {
		return sb.append('}').toString();
            }
	    sb.append(", ");
	}
    }

    transient volatile Set<K>        keySet = null;
    transient volatile Collection<V> values = null;
    transient volatile Set<Map.Entry<K,V>> entrySet = null;
}
    