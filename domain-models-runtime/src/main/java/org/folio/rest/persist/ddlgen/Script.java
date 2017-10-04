package org.folio.rest.persist.ddlgen;

/**
 * @author shale
 *
 */
public class Script {

  private String run;
  private String snippet;
  private double fromModuleVersion;

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
  public double getFromModuleVersion() {
    return fromModuleVersion;
  }
  public void setFromModuleVersion(double fromModuleVersion) {
    this.fromModuleVersion = fromModuleVersion;
  }
}
