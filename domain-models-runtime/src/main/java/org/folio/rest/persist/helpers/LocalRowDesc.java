package org.folio.rest.persist.helpers;

import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.desc.RowDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LocalRowDesc implements RowDescriptor {
  private final ColumnDescriptor[] columnDescriptors;
  private List<String> columnNames;
  private List<ColumnDescriptor> columnDescriptorsList;

  public LocalRowDesc(ColumnDescriptor[] columnDescriptors) {
    this.columnDescriptors = columnDescriptors;
  }

  public LocalRowDesc(List<String> columnNames) {
    this(columnDescriptors(columnNames));
  }

  private static ColumnDescriptor [] columnDescriptors(List<String> columnNames) {
    ColumnDescriptor [] columnDescriptors = new ColumnDescriptor [columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      columnDescriptors[i] = new LocalColumnDescriptor(columnNames.get(i));
    }
    return columnDescriptors;
  }

  @Override
  public List<String> columnNames() {
    if (columnNames == null) {
      columnNames = new ArrayList<>(columnDescriptors.length);
      for (var descriptor : columnDescriptors) {
        columnNames.add(descriptor.name());
      }
      columnNames = Collections.unmodifiableList(columnNames);
    }
    return columnNames;
  }

  @Override
  public List<ColumnDescriptor> columnDescriptors() {
    if (columnDescriptorsList == null) {
      columnDescriptorsList = Collections.unmodifiableList(Arrays.asList(columnDescriptors));
    }
    return columnDescriptorsList;
  }

  public List<ColumnDescriptor> columnDescriptor() {
    return columnDescriptors();
  }

  @Override
  public int columnIndex(String columnName) {
    if (columnName == null) {
      throw new NullPointerException("columnName must not be null");
    }
    return columnNames().indexOf(columnName);
  }

}
