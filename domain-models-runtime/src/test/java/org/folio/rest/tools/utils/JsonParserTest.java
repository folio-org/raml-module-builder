package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.parser.JsonPathParser;
import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonParserTest {

  @Test
  public void check() {

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

    Map<Object, Object> p = jp.getValueAndParentPair("c.arr[0].a2.'aaa.ccc'"); //fix
    assertTrue(p.containsKey("aaa.bbb"));
    assertTrue(((JsonObject)p.get("aaa.bbb")).containsKey("arr2"));

    Map<Object, Object> p2 = jp.getValueAndParentPair("c.arr[3].a2.arr2[2].arr3[1]");
    assertNull(p2);

    Map<Object, Object> p3 = jp.getValueAndParentPair("c.arr[0]");
    assertTrue(p3.containsKey(jp.getValueAt("c.arr[0]")));

    assertEquals(new JsonArray("[{\"a32\":\"5\"}, {\"a32\":\"6\"}]").encode().replace(" ", ""),
      (new JsonArray(((List)jp.getValueAt("c.arr[*].a2.arr2[*].arr3[*]"))).encode().replaceAll(" ", "")));

    String v1 = (String) ((List)jp.getValueAt("c.arr[*].a2.'aaa.ccc'")).get(0);
    assertEquals("aaa.bbb", v1);
    jp.setValueAt("c.arr[0].a2.'aaa.ccc'", "aaa.ccc");
    jp.setValueAt("c.arr[2].a2.'aaa.ccc'", "aaa.ddd");
    String v2 = (String) ((List)jp.getValueAt("c.arr[*].a2.'aaa.ccc'")).get(0);
    assertEquals("aaa.ccc", v2);
    String v3 = (String) ((List)jp.getValueAt("c.arr[*].a2.'aaa.ccc'")).get(2);
    assertEquals("aaa.ddd", v3);

    assertEquals(((List)jp.getValueAt("c.arr[*].a2.arr2[*]")).size(), 5);
    jp.setValueAt("c.arr[0].a2.arr2[0]", "yyy");
    String v4 = (String) ((List)jp.getValueAt("c.arr[*].a2.arr2[*]")).get(0);
    assertEquals("yyy", v4);

    //check nulls
    assertNull(jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));
    jp.setValueAt("c.arr[1].a2.'aaa.ccc'", "aaa.ddd");
    assertNull(jp.getValueAt("c.arr[1].a2.'aaa.ccc'"));

    assertEquals("aaa.ccc", jp.getValueAt("c.arr[0].a2.'aaa.ccc'"));

    assertEquals(2, ((JsonObject)jp.getValueAt("c.arr[0].a2.arr2[2]")).getJsonArray("arr3").size());
    jp.setValueAt("c.arr[0].a2", new JsonObject("{\"xqq\":\"xaa\"}"));
    assertEquals("{\"xqq\":\"xaa\"}", ((JsonObject)jp.getValueAt("c.arr[0].a2")).encode());

    jp.setValueAt("c.arr[*]", new JsonObject());
  }

}
