package org.garret.perst.impl;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.garret.perst.Assert;
import org.garret.perst.GenericIndex;
import org.garret.perst.Index;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;
import org.garret.perst.PersistentIterator;
import org.garret.perst.RegexIndex;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

public class RegexIndexImpl<T> extends AltBtreeFieldIndex<T> implements RegexIndex<T> 
{
    int nGrams;
    boolean caseInsensitive;
    Index<Set<T>> inverseIndex;

    RegexIndexImpl() {}
    
    RegexIndexImpl(StorageImpl db, Class cls, String fieldName, boolean caseInsensitive, int nGrams) {
        super(cls, fieldName, false);
        if (type != ClassDescriptor.tpString) { 
            throw new StorageError(StorageError.INCOMPATIBLE_KEY_TYPE);
        }
        this.caseInsensitive = caseInsensitive;
        this.nGrams = nGrams;
        assignOid(db, 0, false);
        inverseIndex = db.<Set<T>>createIndex(String.class, true);
    }

    
    String[] splitText(String s) {
        int n = s.length() - nGrams + 1;
        if (n < 0) { 
            n = 0;
        }
        String[] ngrams = new String[n];
        char[] ngram = new char[nGrams];
        for (int i = 0; i < n; i++) { 
            for (int j = 0; j < nGrams; j++) { 
                ngram[j] = s.charAt(i + j);
            }
            ngrams[i] = new String(ngram);
        }
        return ngrams;
    }
                
    List<String> splitPattern(String s) { 
        List<String> list = new ArrayList<String>();
        int len = s.length();
        int n = len - nGrams + 1;
        char[] ngram = new char[nGrams];
      forAllNGrams:
        for (int i = 0; i < n; i++) { 
            for (int j = 0; j < nGrams; j++) { 
                char ch = s.charAt(i + j);
                if (ch == '\\') { 
                    if (i + j + 1 == len) { 
                        return list;
                    }
                    ch = s.charAt(i + j + 1);
                }  else if (ch == '_' || ch == '%') { 
                    i += j;
                    continue forAllNGrams;
                }
                ngram[j] = ch;
            }
            list.add(new String(ngram));
        }
        return list;
    }
                
    @Override
    protected Key extractKey(Object obj) { 
        String text = extractText(obj);
        return text != null ? new Key(text) : null;
    }

    private String extractText(Object obj) { 
        try { 
            String text = (String)fld.get(obj);
            if (text != null && caseInsensitive) { 
                text = text.toLowerCase();
            }
            return text;
        } catch (Exception x) { 
            throw new StorageError(StorageError.ACCESS_VIOLATION, x);
        }
    }
           
    @Override
    public boolean put(T obj) {
        String text = extractText(obj);
        if (text != null) { 
            super.insert(new Key(text), obj, false);
            insertInInverseIndex(text, obj);
            return true;
        }
        return false;
    }

    @Override
    public T set(T obj) {
        throw new UnsupportedOperationException("RegexIndex.set(T obj)");       
    }

    private void insertInInverseIndex(String text, T obj) {
        for (String s : splitText(text)) {
            Set<T> set = inverseIndex.get(s);
            if (set == null) { 
                set = getStorage().<T>createSet();
                inverseIndex.put(s, set);
            } 
            set.add(obj);
        }
    }

    @Override
    public void remove(Key key, T obj) {
        throw new UnsupportedOperationException("RegexIndex.remove(Key key, T obj)");       
    }
    
    @Override
    public void deallocate() { 
        inverseIndex.deallocateMembers();
        inverseIndex.deallocate();
        super.deallocate();
    }
        
    @Override
    public void clear() {
        super.clear();
        inverseIndex.deallocateMembers();
    }

    @Override
    Key checkKey(Key key) { 
        if (key != null && caseInsensitive) { 
            key = new Key(((String)key.oval).toLowerCase(), key.inclusion != 0);
        }
        return key;
    }  

    @Override
    public boolean remove(Object obj) {
        String text = extractText(obj);
        if (text != null) { 
            if (super.removeIfExists(new Key(text), obj)) { 
                removeFromInverseIndex(text, obj);
                return true;
            }
        }
        return false;
    }
    
    private void removeFromInverseIndex(String text, Object obj) {
        for (String s : splitText(text)) {
            Set<T> set = inverseIndex.get(s);
            Assert.that(set != null);
        }
    }

    static int findWildcard(String pattern) { 
        int i, n = pattern.length();
        for (i = 0; i < n; i++) { 
            char ch = pattern.charAt(i);
            if (ch == '\\') { 
                i += 1;
            } else if (ch == '%' || ch == '_') { 
                return i;
            }
        }
        return -1;
    }

    @Override
    public IterableIterator<T> match(String pattern) {
        if (caseInsensitive) { 
            pattern = pattern.toLowerCase();
        }
        int firstWildcard = findWildcard(pattern);
        if (firstWildcard < 0) { // exact match
            return iterator(pattern, pattern, GenericIndex.ASCENT_ORDER);
        } else if (firstWildcard == pattern.length()-1 && pattern.charAt(firstWildcard) == '%') { // pattern like 'XYZ%': use prefix search 
            return prefixIterator(pattern.substring(0, firstWildcard));
        } else if (firstWildcard >= nGrams*2 || firstWildcard > pattern.length()-nGrams) { // better to use prefix  search
            return new RegexIterator(prefixIterator(pattern.substring(0, firstWildcard)), pattern);
        } else {
            List<String> ngrams = splitPattern(pattern);
            if (ngrams.size() == 0) { // no n-grams: have to use sequential scan 
                return new RegexIterator(iterator(), pattern);
            }
            Set[] sets = new Set[ngrams.size()];
            for (int i = 0; i < sets.length; i++) { 
                Set set = inverseIndex.get(ngrams.get(i));
                if (set == null) { 
                    return new EmptyIterator<T>();
                } 
                sets[i] = set;
            }        
            return new JoinRegexIterator(sets, pattern);
        }
    }

    @Override
    public boolean isCaseInsensitive() { 
        return caseInsensitive;
    }

    static boolean match(String text, String pattern) { 
        int ti = 0, tn = text.length();
        int pi = 0, pn = pattern.length();
        int any = -1;
        int pos = -1;
        while (true) {
            char ch = pi < pn ? pattern.charAt(pi) : '\0';
            if (ch == '%') {
                any = ++pi;
                pos = ti;
            } else if (ti == tn) { 
                return pi == pn;
            } else if (ch == '\\' && pi+1 < pn && pattern.charAt(pi+1) == text.charAt(ti)) {
                ti += 1;
                pi += 2;
            } else if (ch != '\\' && (ch == text.charAt(ti) || ch == '_')) {
                ti += 1;
                pi += 1;
            } else if (any >= 0) {
                ti = ++pos;
                pi = any;
            } else {
                return false;
            }
        }
    }
            
    class RegexIterator<T> extends IterableIterator<T> implements PersistentIterator 
    { 
        private Iterator<T> iterator;
        private T currObj;
        private String pattern;
        private Storage storage;

        RegexIterator(Iterator<T> iterator, String pattern) { 
            storage = getStorage();
            this.iterator = iterator;
            this.pattern = pattern;
        }

        @Override
        public boolean hasNext() { 
            if (currObj == null) { 
                while (iterator.hasNext()) { 
                    T obj = iterator.next();
                    String text = extractText(obj);
                    if (match(text, pattern)) { 
                        currObj = obj;
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
            T obj = currObj;
            currObj = null;
            return obj;
        }
            
        @Override
        public int nextOid() { 
            int oid = 0;
            if (hasNext()) { 
                oid = storage.getOid(currObj);
                currObj = null;
            }
            return oid;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }               

    static class EmptyIterator<T>  extends IterableIterator<T> implements PersistentIterator 
    {
        @Override
        public boolean hasNext() { 
            return false;
        }

        @Override
        public T next() { 
            throw new NoSuchElementException();
        }

        @Override
        public int nextOid() { 
            return 0;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }              
        
    class JoinRegexIterator<T> extends IterableIterator<T> implements PersistentIterator 
    { 
        private PersistentIterator[] iterators;
        private Object  currObj;
        private int     currOid;
        private String  pattern;
        private Storage storage;
        
        JoinRegexIterator(Set[] sets, String pattern) { 
            storage = getStorage();
            this.pattern  = pattern;
            iterators = new PersistentIterator[sets.length];
            for (int i = 0; i < sets.length; i++) { 
                iterators[i] = (PersistentIterator)sets[i].iterator();
            }
        }
        
        @Override
        public boolean hasNext() { 
            if (currObj == null) { 
                int oid1 = 0, oid2;
                int n = iterators.length;
                while (true) { 
                    for (int i = 0, j = 0; i < n; j++, i++) {
                        do { 
                            oid2 = iterators[j % n].nextOid();
                            if (oid2 == 0) { 
                                return false;
                            }
                        } while (oid2 < oid1);

                        if (oid2 > oid1) { 
                            oid1 = oid2;
                            i = 0;
                        }
                    }
                    Object obj = storage.getObjectByOID(oid1);
                    String text = extractText(obj);
                    if (match(text, pattern)) { 
                        currObj = obj;         
                        currOid = oid1;
                        return true;
                    }
                }
            }
            return true;
        }
    
        @Override
        public T next() { 
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            T obj = (T)currObj;
            currObj = null;
            return obj;
        }
            
        @Override
        public int nextOid() { 
            if (hasNext()) { 
                currObj = null;
                return currOid;
            }
            return 0;
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }              
}
