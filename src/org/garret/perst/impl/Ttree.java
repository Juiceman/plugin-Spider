package org.garret.perst.impl;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.garret.perst.IterableIterator;
import org.garret.perst.PersistentCollection;
import org.garret.perst.PersistentComparator;
import org.garret.perst.PersistentIterator;
import org.garret.perst.SortedCollection;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

public class Ttree<T> extends PersistentCollection<T> implements SortedCollection<T> {
    private PersistentComparator<T> comparator;
    private boolean                 unique;
    private TtreePage               root;
    private int                     nMembers;
    
    private Ttree() {} 

    Ttree(Storage db, PersistentComparator<T> comparator, boolean unique) { 
        super(db);
        this.comparator = comparator;
        this.unique = unique;
    }

    /**
     * Get comparator used in this collection
     * @return collection comparator
     */
    @Override
    public PersistentComparator<T> getComparator() { 
        return comparator;
    }

    @Override
    public boolean recursiveLoading() {
        return false;
    }

    @Override
    public T get(Object key) { 
        if (root != null) { 
            ArrayList list = new ArrayList();
            root.find(comparator, key, 1, key, 1, list);
            if (list.size() > 1) { 
                throw new StorageError(StorageError.KEY_NOT_UNIQUE);
            } else if (list.size() == 0) { 
                return null;
            } else { 
                return (T)list.get(0);
            }
        }
        return null;
    }
            
    @Override
    public ArrayList<T> getList(Object from, Object till) { 
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(comparator, from, 1, till, 1, list);
        }
        return list;
    }

    @Override
    public ArrayList<T> getList(Object from, boolean fromInclusive, Object till, boolean tillInclusive) { 
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(comparator, from, fromInclusive ? 1 : 0, till, tillInclusive ? 1 : 0, list);
        }
        return list;
    }


    @Override
    public Object[] get(Object from, Object till) { 
        return getList(from, till).toArray();
    }

    @Override
    public Object[] get(Object from, boolean fromInclusive, Object till, boolean tillInclusive) { 
        return getList(from, fromInclusive, till, tillInclusive).toArray();
    }

    /**
     * Add new member to collection
     * @param obj new member
     * @return <code>true</code> if object is successfully added in the index, 
     * <code>false</code> if collection was declared as unique and there is already member with such value
     * of the key in the collection. 
     */
    @Override
    public boolean add(T obj) { 
        TtreePage newRoot;
        if (root == null) { 
            newRoot = new TtreePage(getStorage(), obj);
        } else { 
            TtreePage.PageReference ref = new TtreePage.PageReference(root);
            if (root.insert(comparator, obj, unique, ref) == TtreePage.NOT_UNIQUE) { 
                return false;
            }
            newRoot = ref.pg;
        }
        root = newRoot;
        nMembers += 1;
        modify();
        return true;
    }
                
                
    /**
     * Check if collections contains specified member
     * @return <code>true</code> if specified member belongs to the collection
     */
    @Override
    public boolean containsObject(T member) {
        return (root != null && member != null)  ? root.containsObject(comparator, member) : false;
    }    
    
    @Override
    public boolean contains(Object member) {
        return (root != null && member != null) ? root.contains(comparator, member) : false;
    } 
       
    @Override
    public boolean containsKey(Object key) {
        return (root != null && key != null) ? root.containsKey(comparator, key) : false;
    } 


    /**
     * Remove member from collection
     * @param obj member to be removed
     * @return <code>true</code> in case of success, <code>false</code> if there is no such key in the collection
     */
    @Override
    public boolean remove(Object obj) {
        if (root != null) {
            TtreePage.PageReference ref = new TtreePage.PageReference(root);
            if (root.remove(comparator, obj, ref) != TtreePage.NOT_FOUND) {                         
                root = ref.pg;
                nMembers -= 1;        
                modify();
                return true;
            }
        }
        return false;
    }

    /**
     * Get number of objects in the collection
     * @return number of objects in the collection
     */
    @Override
    public int size() {
        return nMembers;
    }
    
    /**
     * Remove all objects from the collection
     */
    @Override
    public void clear() {
        if (root != null) { 
            root.prune();
            root = null;
            nMembers = 0;
            modify();
        }
    }
 
    /**
     * T-Tree destructor
     */
    @Override
    public void deallocate() {
        if (root != null) { 
            root.prune();
        }
        super.deallocate();
    }

    /**
     * Get all objects in the index as array ordered by index key.
     * @return array of objects in the index ordered by key value
     */
    static final Object[] emptySelection = new Object[0];

    @Override
    public Object[] toArray() {
        if (root == null) { 
            return emptySelection;
        }
        Object[] arr = new Object[nMembers];
        root.toArray(arr, 0);
        return arr;
    }

    /**
     * Get all objects in the index as array ordered by index key.
     * The runtime type of the returned array is that of the specified array.  
     * If the index fits in the specified array, it is returned therein.  
     * Otherwise, a new array is allocated with the runtime type of the 
     * specified array and the size of this index.<p>
     *
     * If this index fits in the specified array with room to spare
     * (i.e., the array has more elements than this index), the element
     * in the array immediately following the end of the index is set to
     * <tt>null</tt>.  This is useful in determining the length of this
     * index <i>only</i> if the caller knows that this index does
     * not contain any <tt>null</tt> elements.)<p>
     * @return array of objects in the index ordered by key value
     */
    @Override
    public <E> E[] toArray(E[] arr) {
        if (arr.length < nMembers) { 
            arr = (E[])Array.newInstance(arr.getClass().getComponentType(), nMembers);
        }
        if (root != null) { 
            root.toArray(arr, 0);
        }
        if (arr.length > nMembers) { 
            arr[nMembers] = null;
        }
        return arr;
    }

    class TtreeIterator<T> extends IterableIterator<T> implements PersistentIterator { 
        int           i;
        ArrayList     list;
        boolean       removed;

        TtreeIterator(ArrayList list) { 
            this.list = list;
            i = -1;
        }
        
        @Override
        public T next() { 
            if (i+1 >= list.size()) { 
                throw new NoSuchElementException();
            }
            removed = false;
            return (T)list.get(++i);
        }
        
        @Override
        public int nextOid() { 
            if (i+1 >= list.size()) { 
                return 0;
            }
            removed = false;
            return getStorage().getOid(list.get(++i));
        }
        
        @Override
        public void remove() { 
            if (removed || i < 0 || i >= list.size()) { 
                throw new IllegalStateException();
            }
            Ttree.this.remove(list.get(i));
            list.remove(i--);
            removed = true;
        }
            
        @Override
        public boolean hasNext() {
            return i+1 < list.size();
        }
    }
        
    @Override
    public Iterator<T> iterator() {
        return iterator(null, null);
    }

    @Override
    public IterableIterator<T> iterator(Object from, Object till) {
        return iterator(from, true, till, true);
    }

    @Override
    public IterableIterator<T> iterator(Object from, boolean fromInclusive, Object till, boolean tillInclusive) {
        ArrayList list = new ArrayList();
        if (root != null) { 
            root.find(comparator, from, fromInclusive ? 1 : 0, till, tillInclusive ? 1 : 0, list);
        }            
        return new TtreeIterator(list);
    }

}

