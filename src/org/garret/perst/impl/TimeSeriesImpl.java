package org.garret.perst.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.garret.perst.Assert;
import org.garret.perst.GenericIndex;
import org.garret.perst.Index;
import org.garret.perst.IterableIterator;
import org.garret.perst.Key;
import org.garret.perst.PersistentCollection;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;
import org.garret.perst.TimeSeries;

public class TimeSeriesImpl<T extends TimeSeries.Tick> extends PersistentCollection<T>
    implements TimeSeries<T> {
  class TimeSeriesIterator extends IterableIterator<T> {
    private Iterator blockIterator;

    private Block currBlock;

    private int pos;

    private long till;

    TimeSeriesIterator(long from, long till) {
      pos = -1;
      this.till = till;
      blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till),
          GenericIndex.ASCENT_ORDER);
      while (blockIterator.hasNext()) {
        Block block = (Block) blockIterator.next();
        int n = block.used;
        Tick[] e = block.getTicks();
        int l = 0, r = n;
        while (l < r) {
          int i = (l + r) >> 1;
          if (from > e[i].getTime()) {
            l = i + 1;
          } else {
            r = i;
          }
        }
        Assert.that(l == r && (l == n || e[l].getTime() >= from));
        if (l < n) {
          if (e[l].getTime() <= till) {
            pos = l;
            currBlock = block;
          }
          return;
        }
      }
    }
    @Override
    public boolean hasNext() {
      return pos >= 0;
    }
    @Override
    public T next() {
      if (pos < 0) {
        throw new NoSuchElementException();
      }
      T tick = (T) currBlock.getTicks()[pos];
      if (++pos == currBlock.used) {
        if (blockIterator.hasNext()) {
          currBlock = (Block) blockIterator.next();
          pos = 0;
        } else {
          pos = -1;
          return tick;
        }
      }
      if (currBlock.getTicks()[pos].getTime() > till) {
        pos = -1;
      }
      return tick;
    }
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  class TimeSeriesReverseIterator extends IterableIterator<T> {
    private Iterator blockIterator;

    private Block currBlock;

    private int pos;

    private long from;

    TimeSeriesReverseIterator(long from, long till) {
      pos = -1;
      this.from = from;
      blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till),
          GenericIndex.DESCENT_ORDER);
      while (blockIterator.hasNext()) {
        Block block = (Block) blockIterator.next();
        int n = block.used;
        Tick[] e = block.getTicks();
        int l = 0, r = n;
        while (l < r) {
          int i = (l + r) >> 1;
          if (till >= e[i].getTime()) {
            l = i + 1;
          } else {
            r = i;
          }
        }
        Assert.that(l == r && (l == n || e[l].getTime() > till));
        if (l > 0) {
          if (e[l - 1].getTime() >= from) {
            pos = l - 1;
            currBlock = block;
          }
          return;
        }
      }
    }
    @Override
    public boolean hasNext() {
      return pos >= 0;
    }
    @Override
    public T next() {
      if (pos < 0) {
        throw new NoSuchElementException();
      }
      T tick = (T) currBlock.getTicks()[pos];
      if (--pos < 0) {
        if (blockIterator.hasNext()) {
          currBlock = (Block) blockIterator.next();
          pos = currBlock.used - 1;
        } else {
          pos = -1;
          return tick;
        }
      }
      if (currBlock.getTicks()[pos].getTime() < from) {
        pos = -1;
      }
      return tick;
    }
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private Index index;

  private long maxBlockTimeInterval;

  private String blockClassName;

  private transient Class blockClass;


  TimeSeriesImpl() {}

  TimeSeriesImpl(Storage storage, Class blockClass, long maxBlockTimeInterval) {
    this.blockClass = blockClass;
    this.maxBlockTimeInterval = maxBlockTimeInterval;
    blockClassName = ClassDescriptor.getClassName(blockClass);
    index = storage.createIndex(long.class, false);
  }

  @Override
  public boolean add(T tick) {
    return add(tick, false);
  }

  @Override
  public boolean add(T tick, boolean reverse) {
    long time = tick.getTime();
    Iterator iterator = index.iterator(new Key(time - maxBlockTimeInterval), new Key(time),
        GenericIndex.DESCENT_ORDER);
    if (iterator.hasNext()) {
      insertInBlock((Block) iterator.next(), tick, reverse);
    } else {
      addNewBlock(tick, reverse);
    }
    return true;
  }

  private void addNewBlock(Tick t, boolean reverse) {
    Block block;
    try {
      block = (Block) blockClass.newInstance();
    } catch (Exception x) {
      throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, blockClass, x);
    }
    block.timestamp = t.getTime() - (reverse ? maxBlockTimeInterval : 0);
    block.used = 1;
    block.getTicks()[0] = t;
    index.put(new Key(block.timestamp), block);
  }

  @Override
  public void clear() {
    Iterator blockIterator = index.iterator();
    while (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      block.deallocate();
    }
    index.clear();
  }

  @Override
  public long countTicks() {
    long n = 0;
    Iterator blockIterator = index.iterator();
    while (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      n += block.used;
    }
    return n;
  }

  @Override
  public void deallocate() {
    clear();
    index.deallocate();
    super.deallocate();
  }

  @Override
  public void deallocateMembers() {
    clear();
  }

  @Override
  public ArrayList<T> elements() {
    return new ArrayList<T>(this);
  }

  @Override
  public Date getFirstTime() {
    Iterator blockIterator = index.iterator();
    if (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      return new Date(block.timestamp);
    }
    return null;
  }

  @Override
  public Date getLastTime() {
    Iterator blockIterator = index.iterator(null, null, GenericIndex.DESCENT_ORDER);
    if (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      return new Date(block.getTicks()[block.used - 1].getTime());
    }
    return null;
  }

  @Override
  public T getTick(Date timestamp) {
    long time = timestamp.getTime();
    Iterator blockIterator = index.iterator(new Key(time - maxBlockTimeInterval), new Key(time),
        GenericIndex.ASCENT_ORDER);
    while (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      int n = block.used;
      Tick[] e = block.getTicks();
      int l = 0, r = n;
      while (l < r) {
        int i = (l + r) >> 1;
        if (time > e[i].getTime()) {
          l = i + 1;
        } else {
          r = i;
        }
      }
      Assert.that(l == r && (l == n || e[l].getTime() >= time));
      if (l < n && e[l].getTime() == time) {
        return (T) e[l];
      }
    }
    return null;
  }

  @Override
  public boolean has(Date timestamp) {
    return getTick(timestamp) != null;
  }

  void insertInBlock(Block block, Tick tick, boolean reverse) {
    long t = tick.getTime();
    int i, n = block.used;

    Tick[] e = block.getTicks();
    int l = 0, r = n;
    while (l < r) {
      i = (l + r) >> 1;
      if (t >= e[i].getTime()) {
        l = i + 1;
      } else {
        r = i;
      }
    }
    Assert.that(l == r && (l == n || e[l].getTime() >= t));
    if (r == 0) {
      if (t < block.timestamp || t - block.timestamp > maxBlockTimeInterval || n == e.length) {
        if (reverse && n == e.length) {
          index.remove(new Key(block.timestamp), block);
          block.timestamp = e[0].getTime();
          index.put(new Key(block.timestamp), block);
          block.modify();
        }
        addNewBlock(tick, reverse);
        return;
      }
    } else if (r == n) {
      if (t - block.timestamp > maxBlockTimeInterval || n == e.length) {
        addNewBlock(tick, reverse);
        return;
      }
    }
    if (n == e.length) {
      addNewBlock(e[n - 1], reverse);
      for (i = n; --i > r;) {
        e[i] = e[i - 1];
      }
    } else {
      for (i = n; i > r; i--) {
        e[i] = e[i - 1];
      }
      block.used += 1;
    }
    e[r] = tick;
    block.modify();
  }

  @Override
  public Iterator<T> iterator() {
    return iterator(null, null, true);
  }

  @Override
  public IterableIterator<T> iterator(boolean ascent) {
    return iterator(null, null, ascent);
  }

  @Override
  public IterableIterator<T> iterator(Date from, Date till) {
    return iterator(from, till, true);
  }

  @Override
  public IterableIterator<T> iterator(Date from, Date till, boolean ascent) {
    long low = from == null ? 0 : from.getTime();
    long high = till == null ? Long.MAX_VALUE : till.getTime();
    return ascent ? (IterableIterator<T>) new TimeSeriesIterator(low, high)
        : (IterableIterator<T>) new TimeSeriesReverseIterator(low, high);
  }


  @Override
  public void onLoad() {
    blockClass = ClassDescriptor.loadClass(getStorage(), blockClassName);
  }

  @Override
  public int remove(Date from, Date till) {
    long low = from == null ? 0 : from.getTime();
    long high = till == null ? Long.MAX_VALUE : till.getTime();
    int nRemoved = 0;
    Key fromKey = new Key(low - maxBlockTimeInterval);
    Key tillKey = new Key(high);
    Iterator blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
    while (blockIterator.hasNext()) {
      Block block = (Block) blockIterator.next();
      int n = block.used;
      Tick[] e = block.getTicks();
      int l = 0, r = n;
      while (l < r) {
        int i = (l + r) >> 1;
        if (low > e[i].getTime()) {
          l = i + 1;
        } else {
          r = i;
        }
      }
      Assert.that(l == r && (l == n || e[l].getTime() >= low));
      while (r < n && e[r].getTime() <= high) {
        r += 1;
        nRemoved += 1;
      }
      if (l == 0 && r == n) {
        index.remove(new Key(block.timestamp), block);
        blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
        block.deallocate();
      } else if (l < n && l != r) {
        if (l == 0) {
          index.remove(new Key(block.timestamp), block);
          block.timestamp = e[r].getTime();
          index.put(new Key(block.timestamp), block);
          blockIterator = index.iterator(fromKey, tillKey, GenericIndex.ASCENT_ORDER);
        }
        while (r < n) {
          e[l++] = e[r++];
        }
        block.used = l;
        block.modify();
      }
    }
    return nRemoved;
  }
  @Override
  public int size() {
    return (int) countTicks();
  }
  @Override
  public Object[] toArray() {
    return elements().toArray();
  }
  @Override
  public <E> E[] toArray(E[] arr) {
    return elements().toArray(arr);
  }
}

