package org.folio.rest.persist.criteria;

import static org.junit.Assert.assertEquals;

import org.folio.rest.persist.Criteria.Criteria;
import org.junit.Test;

/**
 * @author shale
 *
 */
public class CriteriaTest {

  @Test
  public void testCriteria() {
    try {
      Criteria noOp = new Criteria();
      noOp.addField("'personal'");
      assertEquals("", noOp.toString());

      Criteria noField = new Criteria();
      noField.setOperation("=");
      assertEquals("", noField.toString());

      Criteria schema = new Criteria();
      schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
      assertEquals("(jsonb->'personal'->>'lastName') =  '123'", schema.toString());

      schema = new Criteria();
      schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
      schema.setAlias("foo");
      assertEquals("(foo.jsonb->'personal'->>'lastName') =  '123'", schema.toString());

      schema = new Criteria();
      schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
      assertEquals("(jsonb->'personal'->>'lastName') =  '123'", schema.toString());
      schema.setNotQuery(true);
      assertEquals("( NOT (jsonb->'personal'->>'lastName') =  '123')", schema.toString());

      schema = new Criteria();
      schema.addField("'personal'").setOperation("@>").setValue("{\"a\":\"b\"}");
      assertEquals("(jsonb->'personal') @>  '{\"a\":\"b\"}'" , schema.toString());

      schema = new Criteria();
      schema.setAlias("'personal'").addField("'foo'").setValue("123");
      assertEquals("('personal'.jsonb->>'foo')", schema.toString());

      //guess op type is string since not null, numeric or boolean
      Criteria c = new Criteria();
      c.addField("'price'").addField("'po_currency'").addField("'value'");
      c.setOperation("like");
      c.setValue("USD");
      assertEquals("(jsonb->'price'->'po_currency'->>'value') like  'USD'", c.toString().trim());

      //guess op type is boolean by checking operation
      Criteria d = new Criteria();
      d.addField("'rush'");
      d.setOperation("IS FALSE");
      d.setValue(null);
      assertEquals("(jsonb->>'rush') IS FALSE null", d.toString().trim());

      //guess op type is boolean by checking value
      Criteria aa = new Criteria();
      aa.addField("'rush'");
      aa.setOperation( "!=" );
      aa.setValue( "true" );
      assertEquals("(jsonb->>'rush') !=  'true'", aa.toString().trim());


      Criteria nb = new Criteria();
      nb.addField("'transaction'");
      nb.addField("'status'");
      nb.setOperation("=");
      nb.setValue("rollbackComplete");
      nb.setArray(true);
      assertEquals("(transaction->'status') =  'rollbackComplete'", nb.toString());
      assertEquals("transaction" , nb.getSelect().getSnippet());
      assertEquals("jsonb_array_elements(jsonb->'transaction')", nb.getFrom().getSnippet());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
