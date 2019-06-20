package org.folio.cql2pgjson.model;

import java.util.List;
import org.folio.rest.persist.ddlgen.Modifier;

public class DbIndex {

  private boolean ft;
  private boolean gin;
  private boolean other;
  private List<Modifier> modifiers;

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

  public List<Modifier> getModifiers() {
    return modifiers;
  }

  public void setModifiers(List<Modifier> modifiers) {
    this.modifiers = modifiers;
  }

}
