package org.folio.cql2pgjson.model;

public class DbIndex {

  private boolean ft;
  private boolean gin;
  private boolean other;
  private boolean foreignKey;

  public boolean isFt() {
    return ft;
  }

  public void setFt(boolean ft) {
    this.ft = ft;
  }

  public boolean isGin() {
    return gin;
  }

  public void setGin(boolean gin) {
    this.gin = gin;
  }

  public boolean isOther() {
    return other;
  }

  public void setOther(boolean other) {
    this.other = other;
  }

  public boolean isForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(boolean foreinKey) {
    this.foreignKey = foreinKey;
  }
}
