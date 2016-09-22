package org.folio.rest.persist.Criteria;

/**
 * @author shale
 *
 */
public class From {


  String snippet;
  String asValue;

  public String getSnippet() {
    return snippet;
  }

  public void setSnippet(String snippet) {
    this.snippet = snippet;
  }

  public String getAsValue() {
    return asValue;
  }

  public void setAsValue(String asValue) {
    this.asValue = asValue;
  }

  @Override
  public String toString() {
    if(snippet == null){
      return "";
    }
    return snippet.toLowerCase().toString();
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof From && obj != null){
      if(snippet != null){
        return snippet.equals(((From)obj).getSnippet());
      }
    }
    return false;
  }

}
