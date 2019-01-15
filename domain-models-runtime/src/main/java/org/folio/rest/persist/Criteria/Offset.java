package org.folio.rest.persist.Criteria;

/**
 * @author shale
 *
 */
public class Offset {

  private final static String OFFSET = "OFFSET";

  private String snippet = "";

  public Offset(int offset){

    snippet = OFFSET + " " + offset;

  }

  public Offset(){

  }

  /**
   * Return the offset as int, return -1 if no limit is set.
   * @return the int
   */
  public int get() {
    if (snippet.isEmpty()) {
      return -1;
    }
    return Integer.parseInt(snippet.substring(7));
  }

  @Override
  public String toString() {
    return snippet;
  }
}
