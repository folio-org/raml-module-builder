package org.folio.rest.persist.helpers;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.RowDesc;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LocalRowSet implements RowSet<Row> {
  private static final Constructor<?> ROW_DESC_CONSTRUCTOR = getRowDescConstructor();
  final int rowCount;
  List<Row> rows = new LinkedList<>();
  RowDesc rowDesc = createRowDesc(Collections.emptyList());

  public LocalRowSet(int rowCount) {
    this.rowCount = rowCount;
  }

  public LocalRowSet withRows(List<Row> rows) {
    this.rows = rows;
    return this;
  }

  public LocalRowSet withColumns(List<String> columns) {
    this.rowDesc = createRowDesc(columns);
    return this;
  }

  /**
   * For Vert.x 4.3.3 and before returns RowDesc(List<String>),
   * for Vert.x 4.3.4 and later returns LocalRowDesc(List<String>).
   */
  private static Constructor<?> getRowDescConstructor() {
    // .getConstructor(new Class[] { List.class })
    // is 20 times slower than this loop when not found because of throwing the exception
    for (Constructor<?> constructor : RowDesc.class.getConstructors()) {
      if (constructor.getParameterCount() == 1 &&
          constructor.getParameters()[0].getType().equals(List.class)) {
        return constructor;
      }
    }
    try {
      return LocalRowDesc.class.getConstructor(new Class[] { List.class });
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private static RowDesc createRowDesc(List<String> columns) {
    try {
      return (RowDesc) ROW_DESC_CONSTRUCTOR.newInstance(columns);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RowIterator<Row> iterator() {
    return new FakeRowIterator(rows);
  }

  @Override
  public int rowCount() {
    return rowCount;
  }

  @Override
  public List<String> columnsNames() {
    return rowDesc.columnNames();
  }

  @Override
  public List<ColumnDescriptor> columnDescriptors() {
    return rowDesc.columnDescriptor();
  }

  @Override
  public int size() {
    return rows.size();
  }

  @Override
  public <V> V property(PropertyKind<V> propertyKind) {
    return null;
  }

  @Override
  public RowSet<Row> value() {
    return null;
  }

  @Override
  public RowSet<Row> next() {
    return null;
  }

  class FakeRowIterator implements RowIterator<Row> {
    final Iterator<Row> iterator;

    FakeRowIterator(List<Row> list) {
      iterator = list.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next();
    }
  }

}
