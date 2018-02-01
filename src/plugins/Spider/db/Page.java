/**
 * @author j16sdiz (1024D/75494252)
 */
package plugins.Spider.db;

import org.garret.perst.FieldIndex;
import org.garret.perst.Persistent;
import org.garret.perst.Storage;
import org.garret.perst.StorageError;
import freenet.support.Logger;

public class Page extends Persistent implements Comparable<Page> {
  /** Page Id */
  protected long id;
  /** URI of the page */
  protected String uri;
  /** Title */
  protected String pageTitle;
  /** Status */
  protected Status status;
  /** Last Change Time */
  protected long lastChange;
  /** Comment, for debugging */
  protected String comment;

  public Page() {}

  Page(String uri, String comment, Storage storage) {
    this.uri = uri;
    this.comment = comment;
    this.status = Status.QUEUED;
    this.lastChange = System.currentTimeMillis();

    storage.makePersistent(this);
  }

  @Override
  public int compareTo(Page o) {
    return new Long(id).compareTo(o.id);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    return id == ((Page) obj).id;
  }

  public String getComment() {
    return comment;
  }

  public long getId() {
    return id;
  }

  public String getPageTitle() {
    return pageTitle;
  }

  public Status getStatus() {
    return status;
  }

  public String getURI() {
    return uri;
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }

  private void postModify() {
    lastChange = System.currentTimeMillis();

    modify();

    Storage storage = getStorage();

    if (storage != null) {
      PerstRoot root = (PerstRoot) storage.getRoot();
      FieldIndex<Page> coll = root.getPageIndex(status);
      coll.exclusiveLock();
      try {
        coll.put(this);
      } finally {
        coll.unlock();
      }
    }
  }

  private void preModify() {
    Storage storage = getStorage();

    if (storage != null) {
      PerstRoot root = (PerstRoot) storage.getRoot();
      FieldIndex<Page> coll = root.getPageIndex(status);
      coll.exclusiveLock();
      try {
        coll.remove(this);
      } catch (StorageError e) {
        if (e.getErrorCode() == StorageError.KEY_NOT_FOUND) {
          // No serious consequences, so just log it, rather than killing the whole thing.
          Logger.error(this, "Page: Key not found in index: " + this, e);
          System.err.println("Page: Key not found in index: " + this);
          e.printStackTrace();
        } else
          throw e;
      } finally {
        coll.unlock();
      }
    }
  }

  public synchronized void setComment(String comment) {
    preModify();
    this.comment = comment;
    postModify();
  }

  public void setPageTitle(String pageTitle) {
    preModify();
    this.pageTitle = pageTitle;
    postModify();
  }

  public synchronized void setStatus(Status status) {
    preModify();
    this.status = status;
    postModify();
  }

  @Override
  public String toString() {
    return "[PAGE: id=" + id + ", title=" + pageTitle + ", uri=" + uri + ", status=" + status
        + ", comment=" + comment + "]";
  }
}
