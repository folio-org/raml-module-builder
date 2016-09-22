package org.folio.rest.persist.Criteria;

/**
 * @author shale
 *
 */
public class Select {

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
    if(obj instanceof Select && obj != null){
      if(snippet != null){
        return snippet.equals(((Select)obj).getSnippet());
      }
    }
    return false;
  }

}
