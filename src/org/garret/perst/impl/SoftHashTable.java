package org.garret.perst.impl;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

public class SoftHashTable extends WeakHashTable {
  public SoftHashTable(StorageImpl db, int initialCapacity) {
    super(db, initialCapacity);
  }

  @Override
  protected Reference createReference(Object obj) {
    return new SoftReference(obj);
  }
}
