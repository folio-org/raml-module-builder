package com.folio.rest.persist.Criteria;

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
  
  @Override
  public String toString() {
    return snippet;
  }
  
}
