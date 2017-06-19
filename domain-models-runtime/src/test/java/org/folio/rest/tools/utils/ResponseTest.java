package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.client.Response;
import org.folio.rest.tools.parser.JsonPathParser;
import org.junit.Test;

import io.vertx.core.json.JsonArray;
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

      //replace value at c.a1 with the value at c.arr[1] and check
      JsonPathParser jpp = new JsonPathParser( test1.joinOn("c.a1", test2, "a", "c.arr[1]").getBody() );
      assertEquals("true", jpp.getValueAt("c.a1.ignore"));
      /////////////////////////////////////////////////////////////


      //non existent path creation test - when passing in a Response as the first parameter
      //we are asking to use the two response parameters as the values to join on, and then populate
      //the response object with a specific value from from the 2nd response parameter
      Response testSetNonExistPath1 = new Response();
      Response r1 = new Response();
      JsonObject j3 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));
      testSetNonExistPath1.setBody(j3);
      //populate r1 with the content from c.arr[1] - this will be the only content in r1
      r1.joinOn(testSetNonExistPath1, "c.a1", test2, "a", "c.arr[1]");
      assertEquals(new JsonObject("{\"c\":{\"a1\":{\"ignore\":\"true\"}}}").encode(), r1.getBody().encode());
      ////////////////////////////////////////////////////////////////


      //try to replace value at c.a1 with a value from a non existent field (c.a[5]) with the
      //param allowNulls set to false so that the value is not updated
      new JsonPathParser(test1.joinOn("c.a1", test2, "a", "c.arr[5]", false).getBody());
      assertEquals(jpp.getValueAt("c.a1.ignore"), "true");

      //non existent insertField with allowNulls set to true (default)
      Response r2 = new Response();
      r2.setBody(j1);
      assertNull(new JsonPathParser(r2.joinOn("c.a1", test2, "a", "c.arr[5]").getBody())
        .getValueAt("c.a1"));
      ////////////////////////////////////////////////////////////////////


      //check non existent path
      assertNull(jpp.getValueAt("c.a1.ignores!SA"));

      //do not allow nulls , so that the c.a9 field which does not exist
      //will not be added since the c.arr[5] is null
      assertFalse(
        test1.joinOn("c.a9", test2, "a", "c.arr[5]" , false).getBody().getJsonObject("c").containsKey("a9"));

      assertTrue(
        test1.joinOn("c.a9", test2, "a", "c.arr[5]" , true).getBody().getJsonObject("c").containsKey("a9"));

      //pass all params to joinOn
      r2.joinOn("b", test2, "c.arr[0].a2.arr2[1].a30", "c.arr[0].a2.arr2[2].arr3[0]", "a", false);
      assertEquals("5" , r2.getBody().getJsonObject("a").getString("a32"));

      String val = (String)new JsonPathParser(
        r2.joinOn("b", test2, "c.arr[0].a2.arr2[1].a30", "c.arr[0]", "a", false).getBody()).getValueAt("a.a3");
      assertEquals("a23" , val);

      //one to many join (a json with one object joined with an array of jsons - where the one json
      //matches multiple entries in the array.
      //json to join on has the same id multiple times with different values for each id.
      //match with this json so that all values are inserted as an array
      j2 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("joinTest.json"), "UTF-8"));

      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));

      Response resp1 = new Response();
      resp1.setBody(j1);

      Response resp2 = new Response();
      resp2.setBody(j2);

      JsonArray arr = (JsonArray)new JsonPathParser(
        resp1.joinOn("a", resp2, "arr2[*].id", "../a31", "b", false).getBody()).getValueAt("b");

      assertEquals(3 , arr.size());

      //test non existent paths (populate new response with array content) one to many
      Response r = new Response();
      r.joinOn(resp1, "a", resp2, "arr2[*].id", "../a31", "b", false);
      JsonArray arr2 =
      (JsonArray)new JsonPathParser(r.getBody()).getValueAt("b");
      assertEquals(new JsonObject("{\"b\":[\"1\",\"2\",\"3\"]}").encode() , r.getBody().encode());
      /////////////////////////////////////////////////////////////////////////


      //test many to many -
      //in the case below will create an array of values for every joined match
      j2 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("joinTest.json"), "UTF-8"));

      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("joinTest.json"), "UTF-8"));

      resp1 = new Response();
      resp1.setBody(j1);

      resp2 = new Response();
      resp2.setBody(j2);

      JsonObject jo1 = resp1.joinOn("arr2[*].id", resp2, "arr2[*].id", "../a31", "../id", false).getBody();
      //will produce (notice that the a31 field value is pushed into the id field for every entry
      //{"arr2":[
      //{"id":["1","2","3"],"a31":"1"},
      //{"id":["1","2","3"],"a31":"2"},
      //{"id":["1","2","3"],"a31":"3"},
      //{"id":["1","2","3"],"a32":"4"},
      //{"id":"4","a31":"4","a32":"5"}
      //]}


      System.out.println(jo1.encode());
      List<?> listOfVals = (List<?>)new JsonPathParser(jo1).getValueAt("arr2[*].id");
      assertEquals(5 , listOfVals.size());
      assertEquals("4", jo1.getJsonArray("arr2").getJsonObject(4).getString("id"));

      //test with numbers / boolean
      j2 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));

      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));

      resp1 = new Response();
      resp1.setBody(j1);

      resp2 = new Response();
      resp2.setBody(j2);

      resp1.joinOn("number", resp2, "number", "boolean", "number", false);
      assertTrue(resp1.getBody().getBoolean("number"));

      resp1.joinOn("boolean", resp2, "boolean", "number", "number", false);
      assertTrue(99 == resp1.getBody().getInteger("number"));

/*      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("compositeTest1.json"), "UTF-8"));*/

      //resp2.setBody(j1);
      Response r2d2 = new Response();
      r2d2.mapFrom(resp1, "c.arr[*]", "compositeUsers.compositeUser", "users", true);
      r2d2.joinOn(r2d2, "compositeUsers.compositeUser[*].users.a3", resp1, "c.arr[*].a3", "c.a1", "../../groups", true);
      assertNull(new JsonPathParser(r2d2.getBody()).getValueAt("compositeUsers.compositeUser[2].groups"));

      Response testMap1 = new Response();

      testMap1.mapFrom(resp1, "c.arr[0].a2", "mapped", null, true);
      assertEquals("aaa.bbb" , new JsonPathParser(testMap1.getBody()).getValueAt("mapped.'aaa.ccc'"));

    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
