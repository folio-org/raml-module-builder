package org.folio.rest.persist.ddlgen;

import org.drools.compiler.kie.builder.impl.KieRepositoryImpl.ComparableVersion;

public abstract class SQLEntity {

  private String fromModuleVersion;

  public String getFromModuleVersion() {
    return fromModuleVersion;
  }
  public void setFromModuleVersion(String fromModuleVersion) {
    this.fromModuleVersion = fromModuleVersion;
  }
  public boolean isNewForThisInstall(String prevVersion) {
    if(fromModuleVersion == null){
      //if no version is specified try to create
      return true;
    }
    ComparableVersion cv = new ComparableVersion(fromModuleVersion);
    int res = cv.compareTo(new ComparableVersion(prevVersion));
    if(res > 0){
      return true;
    }
    return false;
  }

}
