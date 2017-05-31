package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.parser.JsonPathParser;
import org.folio.rest.tools.parser.JsonPathParser.Pairs;
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

    Pairs p = jp.getValueAndParentPair("c.arr[0].a2.'aaa.ccc'"); //fix
    assertEquals("aaa.bbb", p.getRequestedValue());
    assertTrue(((JsonObject)p.getRootNode()).containsKey("a"));

    Pairs p2 = jp.getValueAndParentPair("c.arr[3].a2.arr2[2].arr3[1]");
    assertNull(p2);

    Pairs p3 = jp.getValueAndParentPair("c.arr[0]");
    assertEquals(p3.getRequestedValue() , jp.getValueAt("c.arr[0]"));
    assertEquals("2",((JsonObject)p3.getRootNode()).getString("b"));

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

    assertTrue((Boolean)jp.getValueAt("boolean"));
    assertEquals(99, jp.getValueAt("number"));

    assertEquals(2, ((JsonObject)jp.getValueAt("c.arr[0].a2.arr2[2]")).getJsonArray("arr3").size());
    jp.setValueAt("c.arr[0].a2", new JsonObject("{\"xqq\":\"xaa\"}"));
    assertEquals("{\"xqq\":\"xaa\"}", ((JsonObject)jp.getValueAt("c.arr[0].a2")).encode());

    jp.setValueAt("c.arr[*]", new JsonObject());

    //schema test
    JsonObject j22 = null;
    try {
      j22 = new JsonObject(
        IOUtils.toString(JsonPathParser.class.getClassLoader().
          getResourceAsStream("userdata.json"), "UTF-8"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    JsonPathParser jp2 = new JsonPathParser(j22, true);
    assertEquals("string", jp2.getValueAt("personal.preferredContact.desc.type"));
    assertNull(jp2.getValueAt("personals.preferredContact.desc.type"));
    assertNull(jp2.getValueAt("personal.properties.preferredContact.properties.desc.type"));
    assertEquals("personal.preferredContact.desc.type", jp2.getAbsolutePaths("personal.preferredContact.desc.type").get(0).toString());
    assertEquals("string", jp2.getValueAndParentPair("personal.preferredContact.desc.type").getRequestedValue());
    assertNull(jp2.getValueAndParentPair("personal2.preferredContact.desc.type"));
  }

}
