package org.folio.rest.persist.Criteria;

/**
 * @author shale
 *
 */
public class Limit {

  private final static String LIMIT = "LIMIT";
  private String snippet = "";

  public Limit(int limit){
    snippet = LIMIT + " " + limit;
  }

  public Limit(){
  }

  /**
   * Return the limit as int, return -1 if no limit is set.
   * @return the int
   */
  public int get() {
    if (snippet.isEmpty()) {
      return -1;
    }
    return Integer.parseInt(snippet.substring(6));
  }

  @Override
  public String toString() {
    return snippet;
  }

}
