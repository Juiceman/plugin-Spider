package org.garret.perst.impl;
import java.util.Iterator;
import java.util.Map;
import org.garret.perst.Assert;
import org.garret.perst.Blob;
import org.garret.perst.GenericIndex;
import org.garret.perst.Index;
import org.garret.perst.Key;
import org.garret.perst.Persistent;
import org.garret.perst.PersistentResource;
import org.garret.perst.RandomAccessInputStream;
import org.garret.perst.RandomAccessOutputStream;
import org.garret.perst.Storage;


public class RandomAccessBlobImpl extends PersistentResource implements Blob { 
    long  size;
    Index chunks;

    static class Chunk extends Persistent {
        byte[] body;

        Chunk() {}

        Chunk(Storage db) {
            super(db);
            body = new byte[CHUNK_SIZE];
        }
    }

    static final int CHUNK_SIZE = Page.pageSize - ObjectHeader.sizeof; 

    class BlobInputStream extends RandomAccessInputStream {
        protected Chunk currChunk;
        protected long  currPos;
        protected long  currChunkPos;
        protected Iterator iterator;

        @Override
        public int read() {
            byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        @Override
        public int read(byte b[], int off, int len) {
            if (currPos >= size) { 
                return -1;
            }
            long rest = size - currPos;
            if (len > rest) { 
                len = (int)rest;
            }
            int rc = len;
            while (len > 0) { 
                if (currPos >= currChunkPos + CHUNK_SIZE) {
                    Map.Entry e = (Map.Entry)iterator.next();
                    currChunkPos = ((Long)e.getKey()).longValue();
                    currChunk = (Chunk)e.getValue();
                    Assert.that(currPos < currChunkPos + CHUNK_SIZE);
                }
                if (currPos < currChunkPos) { 
                    int fill = len < currChunkPos - currPos ? len : (int)(currChunkPos - currPos);
                    len -= fill;
                    currPos += fill;
                    while (--fill >= 0) { 
                        b[off++] = 0;
                    }
                }
                int chunkOffs = (int)(currPos - currChunkPos);
                int copy = len < CHUNK_SIZE - chunkOffs ? len : CHUNK_SIZE - chunkOffs;
                System.arraycopy(currChunk.body, chunkOffs, b, off, copy);
                len -= copy;
                off += copy;
                currPos += copy;
            }
            return rc;
        }

        @Override
        public long setPosition(long newPos) { 
            if (newPos < 0) { 
                return -1;
            }
            currPos = newPos > size ? size : newPos;
            iterator = chunks.entryIterator(new Key(currPos/CHUNK_SIZE*CHUNK_SIZE), null, GenericIndex.ASCENT_ORDER);
            currChunkPos = Long.MIN_VALUE;
            currChunk = null;
            return currPos;
        }

        @Override
        public long getPosition() { 
            return currPos;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long skip(long offs) {
            return setPosition(currPos + offs);
        }

        @Override
        public int available() {
            return (int)(size - currPos);
        }

        @Override
        public void close() {
            currChunk = null;
            iterator = null;
        }

        protected BlobInputStream() {  
            setPosition(0);
        }
    }

    class BlobOutputStream extends RandomAccessOutputStream {
        protected Chunk currChunk;
        protected long  currPos;
        protected long  currChunkPos;
        protected Iterator iterator;

        @Override
        public void write(int b) { 
            byte[] buf = new byte[1];
            buf[0] = (byte)b;
            write(buf, 0, 1);
        }

        @Override
        public void write(byte b[], int off, int len) { 
            while (len > 0) {
                boolean newChunk = false;
                if (currPos >= currChunkPos + CHUNK_SIZE) {
                    if (iterator.hasNext()) { 
                        Map.Entry e = (Map.Entry)iterator.next();
                        currChunkPos = ((Long)e.getKey()).longValue();
                        currChunk = (Chunk)e.getValue();
                        Assert.that(currPos < currChunkPos + CHUNK_SIZE);
                    } else { 
                        currChunk = new Chunk(getStorage());
                        currChunkPos = currPos/CHUNK_SIZE*CHUNK_SIZE;
                        newChunk = true;
                    }
                }
                if (currPos < currChunkPos) { 
                    currChunk = new Chunk(getStorage());
                    currChunkPos = currPos/CHUNK_SIZE*CHUNK_SIZE;
                    newChunk = true;
                }
                int chunkOffs = (int)(currPos - currChunkPos);
                int copy = len < CHUNK_SIZE - chunkOffs ? len : CHUNK_SIZE - chunkOffs;
                System.arraycopy(b, off, currChunk.body, chunkOffs, copy);
                len -= copy;
                currPos += copy;
                off += copy;
                if (newChunk) { 
                    chunks.put(new Key(currChunkPos), currChunk);
                    iterator = chunks.entryIterator(new Key(currChunkPos + CHUNK_SIZE), null, GenericIndex.ASCENT_ORDER);
                } else { 
                    currChunk.modify();
                }
            }
            if (currPos > size) { 
                size = currPos;
                modify();
            }
        }

        @Override
        public long setPosition(long newPos) { 
            if (newPos < 0) { 
                return -1;
            }
            currPos = newPos;
            iterator = chunks.entryIterator(new Key(currPos/CHUNK_SIZE*CHUNK_SIZE), null, GenericIndex.ASCENT_ORDER);
            currChunkPos = Long.MIN_VALUE;
            currChunk = null;
            return currPos;
        }

        @Override
        public long getPosition() { 
            return currPos;
        }

        @Override
        public long size() {
            return size;
        }

        public long skip(long offs) {
            return setPosition(currPos + offs);
        }

        @Override
        public void close() {
            currChunk = null;
            iterator = null;
        }

        protected BlobOutputStream(int flags) {  
            setPosition((flags & APPEND) != 0 ? size : 0);
        }
    }

    @Override
    public RandomAccessInputStream getInputStream() { 
        return getInputStream(0);
    }

    @Override
    public RandomAccessInputStream getInputStream(int flags) { 
        return new BlobInputStream();
    }

    @Override
    public RandomAccessOutputStream getOutputStream() { 
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(boolean multisession) { 
        return getOutputStream(0);
    }

    @Override
    public RandomAccessOutputStream getOutputStream(long position, boolean multisession) { 
        RandomAccessOutputStream stream = getOutputStream(multisession);
        stream.setPosition(position);
        return stream;
    }

    @Override
    public RandomAccessOutputStream getOutputStream(int flags) { 
        return new BlobOutputStream(flags);
    }

    @Override
    public void deallocate() { 
        Iterator iterator = chunks.iterator();
        while (iterator.hasNext()) { 
            iterator.next();
            iterator.remove();
        }
        chunks.clear();
        super.deallocate();
    }

    RandomAccessBlobImpl(Storage storage) { 
        super(storage);
        chunks = storage.createIndex(long.class, true);
    }

    RandomAccessBlobImpl() {}
}   