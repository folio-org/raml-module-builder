package org.z3950.zing.cql.cql2pgjson;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.folio.cql2pgjson.model.SqlSelect;
import org.junit.Test;

public class SqlSelectTest {
  @Test
  public void sqlSelectBoth() {
    SqlSelect sqlSelect = new SqlSelect("TRUE", "a");
    assertThat(sqlSelect.getWhere(),   is("TRUE"));
    assertThat(sqlSelect.getOrderBy(), is("a"));
    assertThat(sqlSelect.toString(),   is("WHERE TRUE ORDER BY a"));
  }

  @Test
  public void sqlSelectWhere() {
    SqlSelect sqlSelect = new SqlSelect("false", null);
    assertThat(sqlSelect.getWhere(),   is("false"));
    assertThat(sqlSelect.getOrderBy(), is(""));
    assertThat(sqlSelect.toString(),   is("WHERE false"));
  }

  @Test
  public void sqlSelectOrderBy() {
    SqlSelect sqlSelect = new SqlSelect(null, "a");
    assertThat(sqlSelect.getWhere(),   is(""));
    assertThat(sqlSelect.getOrderBy(), is("a"));
    assertThat(sqlSelect.toString(),   is("ORDER BY a"));
  }

  @Test
  public void sqlSelectNone() {
    SqlSelect sqlSelect = new SqlSelect(null, null);
    assertThat(sqlSelect.getWhere(),   is(""));
    assertThat(sqlSelect.getOrderBy(), is(""));
    assertThat(sqlSelect.toString(),   is(""));
  }
}
