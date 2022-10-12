package org.folio.rest.persist.helpers;

import io.vertx.sqlclient.desc.ColumnDescriptor;
import java.sql.JDBCType;

public class LocalColumnDescriptor implements ColumnDescriptor {
  private final String name;

  public LocalColumnDescriptor(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean isArray() {
    return false;
  }

  @Override
  public String typeName() {
    return null;
  }

  @Override
  public JDBCType jdbcType() {
    return null;
  }
}
