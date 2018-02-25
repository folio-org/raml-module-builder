package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class Script extends Versioned {

  private String run;
  private String snippet;

  public String getRun() {
    return run;
  }
  public void setRun(String run) {
    this.run = run;
  }
  public String getSnippet() {
    return snippet;
  }
  public void setSnippet(String snippet) {
    this.snippet = snippet;
  }

}
