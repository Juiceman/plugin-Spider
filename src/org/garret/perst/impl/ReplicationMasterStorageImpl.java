package org.garret.perst.impl;

import org.garret.perst.IFile;
import org.garret.perst.ReplicationMasterStorage;


public class ReplicationMasterStorageImpl extends StorageImpl implements ReplicationMasterStorage
{ 
    public ReplicationMasterStorageImpl(int port, String[] hosts, int asyncBufSize, String pageTimestampFile) { 
        this.port = port;
        this.hosts = hosts;
        this.asyncBufSize = asyncBufSize;
        this.pageTimestampFile =  pageTimestampFile;
    }
    
    @Override
    public void open(IFile file, long pagePoolSize) {
        super.open(asyncBufSize != 0 
                   ? (ReplicationMasterFile)new AsyncReplicationMasterFile(this, file, asyncBufSize, pageTimestampFile)
                   : new ReplicationMasterFile(this, file, pageTimestampFile),
                   pagePoolSize);
    }

    @Override
    public int getNumberOfAvailableHosts() { 
        return ((ReplicationMasterFile)pool.file).getNumberOfAvailableHosts();
    }

    int      port;
    String[] hosts;
    int      asyncBufSize;
    String   pageTimestampFile;
}
