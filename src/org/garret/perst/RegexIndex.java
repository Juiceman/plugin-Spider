package org.garret.perst;

/**
 * Interface of string field index for regular expression search.
 */
public interface RegexIndex<T> extends FieldIndex<T> {
  /**
   * Locate objects which key matches regular expression
   * 
   * @param regex regular expression with % and _ wildcards
   */
  public IterableIterator<T> match(String regex);
}
