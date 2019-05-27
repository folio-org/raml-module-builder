package org.folio.rest.persist.criteria;

import static org.folio.rest.persist.criteria.CriteriaTest.CriteriaMatcher.isSql;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.folio.rest.persist.Criteria.Criteria;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author shale
 *
 */
public class CriteriaTest {

  @Test
  public void testCriteria() {
    assertThat(new Criteria().addField("'personal'"), isSql(""));

    assertThat(new Criteria().setOperation("="), isSql(""));

    assertThat(new Criteria().addField("'personal'").addField("'lastName'").setOperation("=").setVal("123"),
        isSql("(jsonb->'personal'->>'lastName') = '123'"));

    assertThat(new Criteria().addField("'personal'").addField("'lastName'").setOperation("=").setVal("123").setAlias("foo"),
        isSql("(foo.jsonb->'personal'->>'lastName') = '123'"));

    assertThat(new Criteria().addField("'personal'").addField("'lastName'").setOperation("=").setVal("123").setNotQuery(true),
        isSql("( NOT (jsonb->'personal'->>'lastName') = '123')"));

    assertThat(new Criteria().addField("'personal'").setOperation("@>").setVal("{\"a\":\"b\"}"),
        isSql("(jsonb->'personal') @> '{\"a\":\"b\"}'"));

    assertThat(new Criteria().setAlias("'personal'").addField("'foo'").setVal("123"),
        isSql("('personal'.jsonb->>'foo')"));

    //guess op type is string since not null, numeric or boolean
    assertThat(new Criteria().addField("'price'").addField("'po_currency'").addField("'value'")
        .setOperation("like").setVal("USD"),
        isSql("(jsonb->'price'->'po_currency'->>'value') like 'USD'"));

    //guess op type is boolean by checking operation
    assertThat(new Criteria().addField("'rush'").setOperation("IS FALSE").setVal(null),
        isSql("(jsonb->>'rush') IS FALSE"));

    //guess op type is boolean by checking value
    assertThat(new Criteria().addField("'rush'").setOperation( "!=" ).setVal("true"),
        isSql("(jsonb->>'rush') != 'true'"));

    Criteria nb = new Criteria().addField("'transaction'").addField("'status'")
        .setOperation("=").setVal("rollbackComplete").setArray(true);
    assertThat(nb, isSql("(transaction->'status') =  'rollbackComplete'"));
    assertThat(nb.getSelect().getSnippet(), is("transaction"));
    assertThat(nb.getFrom().getSnippet(), is("jsonb_array_elements(jsonb->'transaction')"));
  }

  @ParameterizedTest
  @MethodSource
  void criteriaValue(String value, String sql) {
    Criteria criteria = new Criteria().addField("'f'").setOperation("=").setVal(value);
    assertThat(criteria, isSql("(jsonb->>'f') = " + sql));
  }

  static Stream<Arguments> criteriaValue() {
    return Stream.of(
        arguments("a",       "'a'"),
        arguments("'a'",     "'''a'''"),
        arguments("O'Kapi",  "'O''Kapi'"),
        arguments("'",       "''''"),
        arguments("''",      "''''''"),
        arguments("Up/\\Up", "'Up/\\Up'"),  // Up/\Up -> 'Up/\Up' because SQL string does not use \ for masking
        arguments("",        "''"),
        arguments(null,      "")
    );
  }

  public static class CriteriaMatcher extends org.hamcrest.BaseMatcher<org.folio.rest.persist.Criteria.Criteria> {
    public String expected;

    public static CriteriaMatcher isSql(String expected) {
        return new CriteriaMatcher(expected);
    }

    private CriteriaMatcher(String expected) {
      this.expected = expected.trim().replaceAll("  +", " ");
    }

    @Override
    public boolean matches(Object actual) {
      return expected.equals(actual.toString().trim().replaceAll("  +", " "));
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(String.format("Criteria toString() should match '%s' ignoring duplicated space", expected));
    }
  }
}
