package org.garret.perst.impl;
import org.garret.perst.*;

public class PersistentStub implements IPersistent { 
    @Override
    public void load() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void loadAndModify() {
        load();
        modify();
    }

    @Override
    public final boolean isRaw() { 
        return true;
    } 
    
    @Override
    public final boolean isModified() { 
        return false;
    } 
    
    @Override
    public final boolean isDeleted() { 
        return false;
    } 
    
    @Override
    public final boolean isPersistent() { 
        return true;
    }
    
    @Override
    public void makePersistent(Storage storage) { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void store() {
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }
  
    @Override
    public void modify() { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    public PersistentStub(Storage storage, int oid) { 
        this.storage = storage;
        this.oid = oid;
    }

    @Override
    public final int getOid() {
        return oid;
    }

    @Override
    public void deallocate() { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public boolean recursiveLoading() {
        return true;
    }
    
    @Override
    public final Storage getStorage() {
        return storage;
    }
    
    @Override
    public boolean equals(Object o) { 
        return getStorage().getOid(o) == oid;
    }

    @Override
    public int hashCode() {
        return oid;
    }

    @Override
    public void onLoad() {
    }

    @Override
    public void onStore() {
    }

    @Override
    public void invalidate() { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    transient Storage storage;
    transient int     oid;

    @Override
    public void unassignOid() { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public void assignOid(Storage storage, int oid, boolean raw) { 
        throw new StorageError(StorageError.ACCESS_TO_STUB);
    }

    @Override
    public Object clone() throws CloneNotSupportedException { 
        PersistentStub p = (PersistentStub)super.clone();
        p.oid = 0;
        return p;
    }

    @Override
    public void readExternal(java.io.ObjectInput s) throws java.io.IOException, ClassNotFoundException
    {
        oid = s.readInt();
    }

    @Override
    public void writeExternal(java.io.ObjectOutput s) throws java.io.IOException
    {
        s.writeInt(oid);
    }
}





