package org.folio.rest.persist.helpers;

import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;
import java.util.List;

public class LocalRowDesc extends RowDesc {
  public LocalRowDesc(ColumnDescriptor[] columnDescriptors) {
    super(columnDescriptors);
  }

  public LocalRowDesc(List<String> columnNames) {
    super(columnDescriptors(columnNames));
  }

  private static ColumnDescriptor [] columnDescriptors(List<String> columnNames) {
    ColumnDescriptor [] columnDescriptors = new ColumnDescriptor [columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      columnDescriptors[i] = new LocalColumnDescriptor(columnNames.get(i));
    }
    return columnDescriptors;
  }
}
