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

  /**
   * Utility to create SemVer class from module version string
   * @param version which may either be a module-version or version
   * @return version component
   */
  private static SemVer moduleVersionToSemVer(String version) {
   try {
      return new SemVer(version);
    } catch (IllegalArgumentException ex) {
      return new ModuleId(version).getSemVer();
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
