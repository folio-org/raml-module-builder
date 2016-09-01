package com.sling.rest.persist.Criteria;

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
  
  @Override
  public String toString() {
    return snippet;
  }
}
