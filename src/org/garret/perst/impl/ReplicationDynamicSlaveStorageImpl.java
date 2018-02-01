package org.garret.perst.impl;

import java.io.IOException;
import java.net.Socket;
import org.garret.perst.IFile;


public class ReplicationDynamicSlaveStorageImpl extends ReplicationSlaveStorageImpl
{
    public ReplicationDynamicSlaveStorageImpl(String host, int port, String pageTimestampFilePath) { 
        super(pageTimestampFilePath);
        this.host = host;
        this.port = port;
    }

    @Override
    public void open(IFile file, long pagePoolSize) {
        initialized = false;
        prevIndex = -1;
        outOfSync = true;
        super.open(file, pagePoolSize);
    }

    @Override
    Socket getSocket() throws IOException { 
        return new Socket(host, port);
    }

    protected String host;
    protected int    port;
}    

    
                                               