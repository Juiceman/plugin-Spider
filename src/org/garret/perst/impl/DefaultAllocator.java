package org.garret.perst.impl;

import org.garret.perst.CustomAllocator;
import org.garret.perst.Persistent;
import org.garret.perst.Storage;

public class DefaultAllocator extends Persistent implements CustomAllocator {
  protected DefaultAllocator() {}

  public DefaultAllocator(Storage storage) {
    super(storage);
  }

  @Override
  public long allocate(long size) {
    return ((StorageImpl) getStorage()).allocate(size, 0);
  }

  @Override
  public void commit() {}

  @Override
  public void free(long pos, long size) {
    ((StorageImpl) getStorage()).cloneBitmap(pos, size);
  }

  @Override
  public long getAllocatedMemory() {
    return getStorage().getDatabaseSize();
  }

  @Override
  public long getSegmentBase() {
    return 0;
  }

  @Override
  public long getSegmentSize() {
    return 1L << StorageImpl.dbLargeDatabaseOffsetBits;
  }

  @Override
  public long getUsedMemory() {
    return getStorage().getUsedSize();
  }

  @Override
  public long reallocate(long pos, long oldSize, long newSize) {
    StorageImpl db = (StorageImpl) getStorage();
    if (((newSize + StorageImpl.dbAllocationQuantum - 1)
        & ~(StorageImpl.dbAllocationQuantum - 1)) > ((oldSize + StorageImpl.dbAllocationQuantum - 1)
            & ~(StorageImpl.dbAllocationQuantum - 1))) {
      long newPos = db.allocate(newSize, 0);
      db.cloneBitmap(pos, oldSize);
      db.free(pos, oldSize);
      pos = newPos;
    }
    return pos;
  }
}


