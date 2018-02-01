package org.garret.perst;

import java.io.IOException;
import java.io.OutputStream;
import org.garret.perst.impl.Page;

/**
 * Stream class implementation on top of Perst file. Can be used in Storage.Backup method
 */
public class IFileOutputStream extends OutputStream {
  IFile file;

  long currPos;

  byte[] page;

  public IFileOutputStream(IFile file) {
    this.file = file;
    page = new byte[Page.pageSize];
  }

  @Override
  public void close() throws IOException {
    flush();
    file.close();
  }

  @Override
  public void flush() throws IOException {
    int dstOff = (int) (currPos % Page.pageSize);
    file.write(currPos - dstOff, page);
  }
  @Override
  public void write(byte b[], int srcOff, int len) throws IOException {
    while (len > 0) {
      int dstOff = (int) (currPos % Page.pageSize);
      int quant = page.length - dstOff > len ? len : page.length - dstOff;
      System.arraycopy(b, srcOff, page, dstOff, quant);
      srcOff += quant;
      dstOff += quant;
      currPos += quant;
      len -= quant;
      if (dstOff == page.length) {
        file.write(currPos - dstOff, page);
      }
    }
  }
  @Override
  public void write(int b) throws IOException {
    byte[] bytes = new byte[1];
    bytes[0] = (byte) b;
    write(bytes, 0, 1);
  }
}
