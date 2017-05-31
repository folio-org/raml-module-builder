package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;

import org.folio.rest.persist.Criteria.Criteria;
import org.junit.Test;

/**
 * @author shale
 *
 */
public class CriteriaTest {

  @Test
  public void testCriteria(){
    try {
      //pass schema so that type is known
      Criteria schema = new Criteria("userdata.json");
      schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
      assertEquals("(jsonb->'personal'->>'lastName') =  '123'", schema.toString());

      //pass schema so that type is known
      schema = new Criteria("userdata.json");
      schema.addField("'active'").setOperation("=").setValue("true");
      assertEquals("(jsonb->>'active')::boolean = true", schema.toString());

      //guess op type is numeric
      schema = new Criteria();
      schema.addField("'personal'").addField("'lastName'").setOperation("=").setValue("123");
      assertEquals("(jsonb->'personal'->>'lastName')::numeric = 123", schema.toString());

      //guess op type is string since not null, numeric or boolean
      Criteria c = new Criteria();
      c.addField("'price'").addField("'po_currency'").addField("'value'");
      c.setOperation("like");
      c.setValue("USD");
      assertEquals("(jsonb->'price'->'po_currency'->>'value') like  'USD'", c.toString().trim());

      //guess op type is boolean by checking operation
      Criteria d = new Criteria();
      d.addField("'rush'");
      d.setOperation( Criteria.OP_IS_FALSE );
      d.setValue(null);
      assertEquals("(jsonb->>'rush')::boolean IS FALSE", d.toString().trim());

      //guess op type is boolean by checking value
      Criteria aa = new Criteria();
      aa.addField("'rush'");
      aa.setOperation( "!=" );
      aa.setValue( "true" );
      assertEquals("(jsonb->>'rush')::boolean != true", aa.toString().trim());


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
