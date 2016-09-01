package com.sling.rest.persist.Criteria;

/**
 * @author shale
 *
 */
public class Order {

  public final static String ASC = "asc";
  public final static String DESC = "desc";
  
  private final static String ORDER = "ORDER BY ";
  
  private String snippet = "";
  
  public enum ORDER {
    ASC ("asc"),
    DESC ("desc");
    
    private final String order;

    private ORDER(String order) {
        this.order = order;
    }
  }

  public Order(){
  }
  
  public Order(String fieldName, ORDER order){
    snippet = ORDER +fieldName +" "+ order.toString();
  }
  
  public void asc(String fieldName){
    snippet = ORDER +fieldName +" "+ ASC; 
  }
  
  public void desc(String fieldName){
    snippet = ORDER +fieldName +" "+ DESC;
  }
  
  @Override
  public String toString() {
    return snippet;
  }
}
