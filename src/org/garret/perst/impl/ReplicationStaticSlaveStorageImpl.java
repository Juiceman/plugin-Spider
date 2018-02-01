package org.garret.perst.impl;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import org.garret.perst.IFile;
import org.garret.perst.StorageError;


public class ReplicationStaticSlaveStorageImpl extends ReplicationSlaveStorageImpl {
  public ReplicationStaticSlaveStorageImpl(int port, String pageTimestampFilePath) {
    super(pageTimestampFilePath);
    this.port = port;
  }

  @Override
  public void open(IFile file, long pagePoolSize) {
    try {
      acceptor = new ServerSocket(port);
    } catch (IOException x) {
      throw new StorageError(StorageError.BAD_REPLICATION_PORT);
    }
    byte[] rootPage = new byte[Page.pageSize];
    int rc = file.read(0, rootPage);
    if (rc == Page.pageSize) {
      prevIndex = rootPage[DB_HDR_CURR_INDEX_OFFSET];
      initialized = rootPage[DB_HDR_INITIALIZED_OFFSET] != 0;
    } else {
      initialized = false;
      prevIndex = -1;
    }
    outOfSync = false;
    super.open(file, pagePoolSize);
  }

  @Override
  Socket getSocket() throws IOException {
    return acceptor.accept();
  }

  // Cancel accept
  @Override
  void cancelIO() {
    try {
      Socket s = new Socket("localhost", port);
      s.close();
    } catch (IOException x) {
    }
  }


  protected ServerSocket acceptor;
  protected int port;
}


