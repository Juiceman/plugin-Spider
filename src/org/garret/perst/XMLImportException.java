package org.garret.perst;

/**
 * Exception thrown during import of data from XML file in database
 */
public class XMLImportException extends Exception {
  private int line;

  private int column;

  private String message;

  public XMLImportException(int line, int column, String message) {
    super("In line " + line + " column " + column + ": " + message);
    this.line = line;
    this.column = column;
    this.message = message;
  }

  public int getColumn() {
    return column;
  }
  public int getLine() {
    return line;
  }
  public String getMessageText() {
    return message;
  }
}
