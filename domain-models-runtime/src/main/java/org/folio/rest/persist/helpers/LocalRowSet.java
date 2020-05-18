package org.folio.rest.persist.helpers;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LocalRowSet implements RowSet<Row> {
  final int rowCount;
  List<Row> rows = new LinkedList<>();
  List<String> columns = new LinkedList<>();

  public LocalRowSet(int rowCount) {
    this.rowCount = rowCount;
  }

  public LocalRowSet withRows(List<Row> rows) {
    this.rows = rows;
    return this;
  }

  public LocalRowSet withColumns(List<String> columns) {
    this.columns = columns;
    return this;
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
    return columns;
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
