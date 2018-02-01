package org.garret.perst.impl;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.garret.perst.Assert;
import org.garret.perst.IterableIterator;
import org.garret.perst.PersistentCollection;
import org.garret.perst.PersistentIterator;
import org.garret.perst.Rectangle;
import org.garret.perst.SpatialIndex;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

public class Rtree<T> extends PersistentCollection<T> implements SpatialIndex<T> {
    private int       height;
    private int       n;
    private RtreePage root;
    private transient int updateCounter;

    Rtree() {}

    @Override
    public void put(Rectangle r, T obj) {
        Storage db = getStorage();
        if (root == null) { 
            root = new RtreePage(db, obj, r);
            height = 1;
        } else { 
            RtreePage p = root.insert(db, r, obj, height); 
            if (p != null) {
                root = new RtreePage(db, root, p);
                height += 1;
            }
        }
        updateCounter += 1;
        n += 1;
        modify();
    }
    
    @Override
    public int size() { 
        return n;
    }

    @Override
    public void remove(Rectangle r, T obj) {
        if (root == null) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) { 
             throw new StorageError(StorageError.KEY_NOT_FOUND);
        }        
        for (int i = reinsertList.size(); --i >= 0;) {
            RtreePage p = (RtreePage)reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) { 
                RtreePage q = root.insert(getStorage(), p.b[j], p.branch.get(j), height - reinsertLevel); 
                if (q != null) { 
                    // root splitted
                    root = new RtreePage(getStorage(), root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) { 
            RtreePage newRoot = (RtreePage)root.branch.get(0);
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        updateCounter += 1;
        modify();
    }
    
    @Override
    public Object[] get(Rectangle r) {
        return getList(r).toArray();
    }

    @Override
    public ArrayList<T> getList(Rectangle r) { 
        ArrayList<T> result = new ArrayList<T>();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result;
    }

    @Override
    public Object[] toArray() {
        return get(getWrappingRectangle());
    }

    @Override
    public <E> E[] toArray(E[] arr) {
        return getList(getWrappingRectangle()).toArray(arr);
    }
    
    @Override
    public Rectangle getWrappingRectangle() {
        if (root != null) { 
            return root.cover();
        }
        return null;
    }

    @Override
    public void clear() {
        if (root != null) { 
            root.purge(height);
            root = null;
        }
        height = 0;
        n = 0;
        modify();
    }

    @Override
    public void deallocate() {
        clear();
        super.deallocate();
    }

    class RtreeIterator<E> extends IterableIterator<E> implements PersistentIterator {
        RtreeIterator(Rectangle r) { 
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            this.r = r;            
            pageStack = new RtreePage[height];
            posStack = new int[height];

            if (!gotoFirstItem(0, root)) { 
                pageStack = null;
                posStack = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            return pageStack != null;
        }

        protected Object current(int sp) { 
            return pageStack[sp].branch.get(posStack[sp]);
        }

        @Override
        public E next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            E curr = (E)current(height-1);
            if (!gotoNextItem(height-1)) { 
                pageStack = null;
                posStack = null;
            }
            return curr;
        }
 
        @Override
        public int nextOid() {
            if (!hasNext()) { 
                return 0;
            }
            int oid = getStorage().getOid(pageStack[height-1].branch.getRaw(posStack[height-1]));
            if (!gotoNextItem(height-1)) { 
                pageStack = null;
                posStack = null;
            }
            return oid;
        }
        
        private boolean gotoFirstItem(int sp, RtreePage pg) { 
            for (int i = 0, n = pg.n; i < n; i++) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreePage)pg.branch.get(i))) { 
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            return false;
        }
              
 
        private boolean gotoNextItem(int sp) {
            RtreePage pg = pageStack[sp];
            for (int i = posStack[sp], n = pg.n; ++i < n;) { 
                if (r.intersects(pg.b[i])) { 
                    if (sp+1 == height || gotoFirstItem(sp+1, (RtreePage)pg.branch.get(i))) { 
                        pageStack[sp] = pg;
                        posStack[sp] = i;
                        return true;
                    }
                }
            }
            pageStack[sp] = null;
            return (sp > 0) ? gotoNextItem(sp-1) : false;
        }
              
        @Override
        public void remove() { 
            throw new UnsupportedOperationException();
        }

        RtreePage[] pageStack;
        int[]       posStack;
        int         counter;
        Rectangle   r;
    }
    
    static class RtreeEntry<T> implements Map.Entry<Rectangle,T> {
        RtreePage pg;
        int       pos;

	@Override
  public Rectangle getKey() {
	    return pg.b[pos];
	}

	@Override
  public T getValue() {
	    return (T)pg.branch.get(pos);
	}

  	@Override
    public T setValue(T value) {
            throw new UnsupportedOperationException();
        }

        RtreeEntry(RtreePage pg, int pos) { 
            this.pg = pg;
            this.pos = pos;
        }
    }
        
    class RtreeEntryIterator extends RtreeIterator<Map.Entry<Rectangle,T>> {
        RtreeEntryIterator(Rectangle r) { 
            super(r);
        }
        
        @Override
        protected Object current(int sp) { 
            return new RtreeEntry(pageStack[sp], posStack[sp]);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle,T>> entryIterator() {
        return entryIterator(getWrappingRectangle());
    }

    @Override
    public IterableIterator<T> iterator(Rectangle r) { 
        return new RtreeIterator<T>(r);
    }

    @Override
    public IterableIterator<Map.Entry<Rectangle,T>> entryIterator(Rectangle r) { 
        return new RtreeEntryIterator(r);
    }

    static class Neighbor { 
        Object   child;
        Neighbor next;
        int      level;
        double   distance;

        Neighbor(Object child, double distance, int level) { 
            this.child = child;
            this.distance = distance;
            this.level = level;
        }
    }

    class NeighborIterator<E> extends IterableIterator<E> implements PersistentIterator 
    {
        Neighbor list;
        int counter;
        int x;
        int y;
        Storage storage;

        NeighborIterator(int x, int y) { 
            this.x = x;
            this.y = y;
            counter = updateCounter;
            if (height == 0) { 
                return;
            }
            list = new Neighbor(root, root.cover().distance(x, y), height);
            storage = getStorage();
        }

        void insert(Neighbor node) { 
            Neighbor prev = null, next = list;
            double distance = node.distance;
            while (next != null && next.distance < distance) { 
                prev = next;
                next = prev.next;
            }
            node.next = next;
            if (prev == null) { 
                list = node;
            } else { 
                prev.next = node;
            }
        }

        @Override
        public boolean hasNext() { 
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            while (true) { 
                Neighbor neighbor = list;
                if (neighbor == null) { 
                    return false;
                }
                if (neighbor.level == 0) { 
                    return true;
                }
                list = neighbor.next;
                RtreePage pg = (RtreePage)(neighbor.child instanceof RtreePage ? neighbor.child : storage.getObjectByOID(storage.getOid(neighbor.child)));
                for (int i = 0, n = pg.n; i < n; i++) { 
                    insert(new Neighbor(pg.branch.getRaw(i), pg.b[i].distance(x, y), neighbor.level-1));
                }
            }
        }

        @Override
        public E next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            Neighbor neighbor = list;
            list = neighbor.next;
            Assert.that(neighbor.level == 0);            
            return (E)(neighbor.child instanceof PersistentStub ? storage.getObjectByOID(storage.getOid(neighbor.child)) : neighbor.child);
        }

        @Override
        public int nextOid() {
            if (!hasNext()) { 
                return 0;
            }
            Neighbor neighbor = list;
            list = neighbor.next;
            Assert.that(neighbor.level == 0);
            return storage.getOid(neighbor.child);
        }

        @Override
        public void remove() { 
            throw new UnsupportedOperationException();
        }
    }
        
    @Override
    public IterableIterator<T> neighborIterator(int x, int y) { 
        return new NeighborIterator(x, y);
    }
}
    

