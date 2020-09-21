package org.folio.dbschema;

/**
 * @author shale
 *
 */
public class Script extends Versioned {

  private String run;
  private String snippet;
  private String snippetPath;

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
  public String getSnippetPath() {
    return snippetPath;
  }
  public void setSnippetPath(String snippetPath) {
    this.snippetPath = snippetPath;
  }
}
