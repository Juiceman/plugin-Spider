package org.garret.perst.impl;

import org.garret.perst.IPersistent;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;

public class PersistentStub implements IPersistent {
  transient Storage storage;

  transient int oid;

  public PersistentStub(Storage storage, int oid) {
    this.storage = storage;
    this.oid = oid;
  }

  @Override
  public void assignOid(Storage storage, int oid, boolean raw) {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PersistentStub p = (PersistentStub) super.clone();
    p.oid = 0;
    return p;
  }

  @Override
  public void deallocate() {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public boolean equals(Object o) {
    return getStorage().getOid(o) == oid;
  }

  @Override
  public final int getOid() {
    return oid;
  }

  @Override
  public final Storage getStorage() {
    return storage;
  }

  @Override
  public int hashCode() {
    return oid;
  }

  @Override
  public void invalidate() {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public final boolean isDeleted() {
    return false;
  }

  @Override
  public final boolean isModified() {
    return false;
  }

  @Override
  public final boolean isPersistent() {
    return true;
  }

  @Override
  public final boolean isRaw() {
    return true;
  }

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
  public void makePersistent(Storage storage) {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public void modify() {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public void onLoad() {}
  @Override
  public void onStore() {}

  @Override
  public void readExternal(java.io.ObjectInput s)
      throws java.io.IOException, ClassNotFoundException {
    oid = s.readInt();
  }

  @Override
  public boolean recursiveLoading() {
    return true;
  }

  @Override
  public void store() {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public void unassignOid() {
    throw new StorageError(StorageError.ACCESS_TO_STUB);
  }

  @Override
  public void writeExternal(java.io.ObjectOutput s) throws java.io.IOException {
    s.writeInt(oid);
  }
}


