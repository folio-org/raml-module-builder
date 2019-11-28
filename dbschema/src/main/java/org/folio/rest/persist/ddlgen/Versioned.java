package org.folio.rest.persist.ddlgen;

import org.folio.okapi.common.ModuleId;
import org.folio.okapi.common.SemVer;

public abstract class Versioned {

  private String fromModuleVersion;

  public String getFromModuleVersion() {
    return fromModuleVersion;
  }
  public void setFromModuleVersion(String fromModuleVersion) {
    this.fromModuleVersion = fromModuleVersion;
  }

  static private SemVer moduleVersionToSemVer(String version) {
    try {
      return new SemVer(version);
    } catch (IllegalArgumentException ex) {
      ModuleId id = new ModuleId(version);
      return id.getSemVer();
    }
  }

  public boolean isNewForThisInstall(String prevVersion) {
    if (fromModuleVersion == null) {
      //if no version is specified try to create
      return true;
    }
    SemVer cv = moduleVersionToSemVer(fromModuleVersion);
    return cv.compareTo(moduleVersionToSemVer(prevVersion)) > 0;
  }
}
