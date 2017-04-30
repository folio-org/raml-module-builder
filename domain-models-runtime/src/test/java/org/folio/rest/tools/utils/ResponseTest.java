package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.parser.JsonPathParser;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class ResponseTest {

  @Test
  public void check() {
    JsonObject j1 = null;
    JsonObject j2 = null;

    try {
      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));

      j2 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));

      Response test1 = new Response();
      test1.setBody(j1);
      Response test2 = new Response();
      test2.setBody(j2);

      JsonPathParser jpp = new JsonPathParser( test1.joinOn("c.a1", test2, "a", "c.arr[1]").getBody() );
      assertEquals(jpp.getValueAt("c.a1.ignore"), "true");

      //check non existent path
      assertNull(jpp.getValueAt("c.a1.ignores!SA"));

      //non existent insertField
      assertNull(new JsonPathParser(test1.joinOn("c.a1", test2, "a", "c.arr[5]").getBody())
        .getValueAt("a.c.a1"));

      //non existant insertField and join field -
      assertNotNull(test1.joinOn("c.a9", test2, "a", "c.arr[5]"));
      //System.out.println(test1.joinOn("c.a9", test2, "a", "c.arr[5]").getBody());

    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
