package org.garret.perst;

/**
 * Base class for all persistent capable objects
 */
public class Persistent extends PinnedPersistent
{ 
    public Persistent() {}

    public Persistent(Storage storage) { 
        super(storage);
    }

    @Override
    protected void finalize() { 
        if ((state & DIRTY) != 0 && oid != 0) { 
            storage.storeFinalizedObject(this);
        }
        state = DELETED;
    }
}





