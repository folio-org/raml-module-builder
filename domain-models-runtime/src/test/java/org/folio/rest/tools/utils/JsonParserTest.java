package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.parser.JsonPathParser;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class JsonParserTest {

  @Test
  public void checkValueAndGetObjectItIsNestedIn() {

    JsonObject j1 = null;
    try {
      j1 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("pathTest.json"), "UTF-8"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    JsonPathParser jp = new JsonPathParser(j1);

    List<StringBuilder> o = jp.getAbsolutePaths("c.arr[*].a2.'aaa.ccc'");
    assertEquals(o.size(), 2);

    List<StringBuilder> o2 = jp.getAbsolutePaths("c.arr[*].a2.arr2[*]");
    assertEquals(o2.size(), 5);

    List<StringBuilder> o3 = jp.getAbsolutePaths("c.arr[*].a2.arr2[*].arr3[*]");
    assertEquals(o3.size(), 2);

    List<StringBuilder> o4 = jp.getAbsolutePaths("c.arr[*]");
    assertEquals(o4.size(), 3);

    List<StringBuilder> o5 = jp.getAbsolutePaths("c.arr[0].a2.'aaa.ccc'");
    assertEquals("c.arr[0].a2.'aaa.ccc'", o5.get(0).toString());

    List<StringBuilder> o6 = jp.getAbsolutePaths("c.a1");
    assertEquals(o6.size(), 1);

    List<StringBuilder> o7 = jp.getAbsolutePaths("c.arr[*].a2.arr2[*].arr3[*].a32");
    assertEquals(o7.size(), 2);

    HashMap<String, Object> p = (HashMap<String, Object>)jp.getValueAndParentPair("c.arr[0].a2.'aaa.ccc'"); //fix
    assertTrue(p.containsKey("aaa.bbb"));
    assertTrue(((JsonObject)p.get("aaa.bbb")).containsKey("arr2"));

    HashMap<String, Object> p2 = (HashMap<String, Object>)jp.getValueAndParentPair("c.arr[3].a2.arr2[2].arr3[1]");
    assertNull(p2);

    JsonObject p3 = (JsonObject)jp.getValueAndParentPair("c.arr[0]");
    assertTrue(p3.containsKey("a3"));
  }

}
