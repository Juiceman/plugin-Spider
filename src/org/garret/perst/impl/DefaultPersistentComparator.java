package org.garret.perst.impl;

import org.garret.perst.PersistentComparator;

public class DefaultPersistentComparator<T extends Comparable> extends PersistentComparator<T> {
  @Override
  public int compareMembers(T m1, T m2) {
    return m1.compareTo(m2);
  }

  @Override
  public int compareMemberWithKey(T mbr, Object key) {
    return mbr.compareTo(key);
  }
}
