package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.text.IsEqualIgnoringWhiteSpace.equalToIgnoringWhiteSpace;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.rest.persist.facets.FacetManager;
import org.folio.rest.persist.facets.ParsedQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class PostgresClientTest {
  // See PostgresClientIT.java for the tests that require a postgres database!

  private static final Logger log = LogManager.getLogger(PostgresClientTest.class);

  @ParameterizedTest
  @ValueSource(strings = {
      //"SELECT * FROM table WHERE items_mt_view.jsonb->>' ORDER BY items_mt_view.jsonb->>\\'aaa\\'  ORDER BY items2_mt_view.jsonb' ORDER BY items_mt_view.jsonb->>'aaa limit' OFFSET 31 limit 10",
      "SELECT * FROM table WHERE items_mt_view.jsonb->>'title' LIKE '%12345%' ORDER BY items_mt_view.jsonb->>'title' DESC OFFSET 30 limit 10",
      "select jsonb,_id FROM counter_mod_inventory_storage.item  WHERE jsonb@>'{\"barcode\":4}' order by jsonb->'a'  asc, jsonb->'b' desc, jsonb->'c'",
      "select jsonb,_id FROM counter_mod_inventory_storage.item  WHERE jsonb @> '{\"barcode\":4}' limit 100 offset 0",
      //"SELECT * FROM table WHERE items0_mt_view.jsonb->>' ORDER BY items1_mt_view.jsonb->>''aaa'' ' ORDER BY items2_mt_view.jsonb->>' ORDER BY items3_mt_view.jsonb->>''aaa'' '",
      "SELECT _id FROM test_tenant_mod_inventory_storage.material_type  WHERE jsonb@>'{\"id\":\"af6c5503-71e7-4b1f-9810-5c9f1af7c570\"}' LIMIT 1 OFFSET 0 ",
      "select * from diku999_circulation_storage.audit_loan WHERE audit_loan.jsonb->>'id' = 'cf23adf0-61ba-4887-bf82-956c4aae2260 order by created_date LIMIT 10 OFFSET 0' order by created_date LIMIT 10 OFFSET 0 ",
      "select * from slowtest99_mod_inventory_storage.item where (item.jsonb->'barcode') = to_jsonb('1000000'::int)  order by a LIMIT 30;",
      "SELECT  * FROM slowtest_cql5_mod_inventory_storage.item  WHERE lower(f_unaccent(item.jsonb->>'default')) LIKE lower(f_unaccent('true')) ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC, item.jsonb->>'code' DESC LIMIT 10 OFFSET 0",
      //"SELECT * FROM harvard_mod_configuration.config_data  WHERE ((true) AND ( (config_data.jsonb->>'userId' ~ '') IS NOT TRUE)) OR (lower(f_unaccent(config_data.jsonb->>'userId')) ~ lower(f_unaccent('(^|[[:punct:]]|[[:space:]])joeshmoe($|[[:punct:]]|[[:space:]])')))  ORDER BY lower(f_unaccent(item.jsonb->>'code')) DESC, item.jsonb->>'code' DESC LIMIT 10 OFFSET 0",
  })
  void parseQuery(String query) throws JSQLParserException {
    long start = System.nanoTime();

    List<String> facets = new ArrayList<>();
    facets.add("barcode");
    facets.add("materialTypeId");
    FacetManager.setCalculateOnFirst(0);
    ParsedQuery pq = PostgresClient.parseQuery(query);
    if (pq.getOrderByClause() == null && pq.getLimitClause() == null && pq.getOrderByClause() == null) {
      fail();
    }

    net.sf.jsqlparser.statement.Statement statement = CCJSqlParserUtil.parse(query);
    Select selectStatement = (Select) statement;
    List<OrderByElement> orderBy = ((PlainSelect) selectStatement.getSelectBody()).getOrderByElements();
    net.sf.jsqlparser.statement.select.Limit limit = ((PlainSelect) selectStatement.getSelectBody()).getLimit();
    net.sf.jsqlparser.statement.select.Offset offset = ((PlainSelect) selectStatement.getSelectBody()).getOffset();

    if (orderBy == null && limit == null && offset == null) {
      fail();
    }

    long end = System.nanoTime();

    log.info(query + " from " + (end-start));
  }

  @Test void parseQuerySimple() {
    ParsedQuery pq = PostgresClient.parseQuery("SELECT * FROM t WHERE TRUE ORDER BY a LIMIT 2 OFFSET 3");
    assertThat(pq.getQueryWithoutLimOff(), equalToIgnoringWhiteSpace("SELECT * FROM t WHERE TRUE ORDER BY a"));
    assertThat(pq.getCountFuncQuery(),     equalToIgnoringWhiteSpace("SELECT * FROM t WHERE TRUE"));
    assertThat(pq.getLimitClause(),        equalToIgnoringWhiteSpace("LIMIT 2"));
    assertThat(pq.getOffsetClause(),       equalToIgnoringWhiteSpace("OFFSET 3"));
    assertThat(pq.getOrderByClause(), is("[a]"));
  }

  @Test void parseWhereOrderBy() {
    ParsedQuery pq = PostgresClient.parseQuery("SELECT * FROM t WHERE TRUE ORDER BY a");
    assertThat(pq.getQueryWithoutLimOff(), equalToIgnoringWhiteSpace("SELECT * FROM t WHERE TRUE ORDER BY a"));
    assertThat(pq.getCountFuncQuery(),     equalToIgnoringWhiteSpace("SELECT * FROM t WHERE TRUE"));
    assertThat(pq.getLimitClause(),        is(nullValue()));
    assertThat(pq.getOffsetClause(),       is(nullValue()));
    assertThat(pq.getOrderByClause(), is("[a]"));
  }

  @Test void parseOffset() {
    ParsedQuery pq = PostgresClient.parseQuery("SELECT * FROM t OFFSET 5");
    assertThat(pq.getQueryWithoutLimOff(), equalToIgnoringWhiteSpace("SELECT * FROM t"));
    assertThat(pq.getCountFuncQuery(),     equalToIgnoringWhiteSpace("SELECT * FROM t"));
    assertThat(pq.getLimitClause(),        is(nullValue()));
    assertThat(pq.getOffsetClause(),       equalToIgnoringWhiteSpace("OFFSET 5"));
    assertThat(pq.getOrderByClause(),      is(nullValue()));
  }

  @Test
  public void deeplyNestedQuery() throws JSQLParserException {
    // causes StackOverflowError in java.util.regex.Pattern:
    // https://issues.folio.org/browse/CIRC-119
    String sql = "(TRUE)";
    for (int i=0; i<10; i++) {
      sql = sql.replaceAll("TRUE", "(TRUE OR TRUE)");
    }

    PostgresClient.parseQuery("SELECT * FROM t WHERE " + sql);
  }
}
