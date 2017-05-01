package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URLDecoder;

import org.folio.rest.tools.client.BuildCQL;
import org.folio.rest.tools.client.Response;
import org.junit.Test;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class BuildCQLTest {

  @Test
  public void check() {


    try {
      JsonObject j = new JsonObject();
      JsonArray j22 = new JsonArray();
      JsonArray j33 = new JsonArray();

      j.put("arr", j22);
      j.put("arr2", j33);

      j22.add("librarian3");
      j22.add("librarian2");

      JsonObject j44 = new JsonObject();
      j44.put("o", new JsonObject("{\"bbb\":\"aaa\"}"));
      j33.add(j44);

      Response r = new Response();
      r.setBody(j);

      String g1 = new BuildCQL(r, "arr2[0]", "group").buildCQL();
      assertEquals("?query=group=={\"o\":{\"bbb\":\"aaa\"}}", URLDecoder.decode(g1, "UTF-8"));
      String g2 = new BuildCQL(r, "arr", "group").buildCQL();
      assertEquals("?query=group==[\"librarian3\",\"librarian2\"]", URLDecoder.decode(g2, "UTF-8"));
      String g3 = new BuildCQL(r, "arr[0]", "group").buildCQL();
      assertEquals("?query=group==librarian3", URLDecoder.decode(g3, "UTF-8"));
      String g4 = new BuildCQL(r, "arr[*]", "group").buildCQL();
      assertEquals("?query=group==librarian3 or group==librarian2", URLDecoder.decode(g4, "UTF-8"));
      String g5 = new BuildCQL(r, "arr[*]", "group", "query1").buildCQL();
      assertEquals("?query1=group==librarian3 or group==librarian2", URLDecoder.decode(g5, "UTF-8"));
      String g6 = new BuildCQL(r, "arr[*]", "group", "query1", false, "and").buildCQL();
      assertEquals("&query1=group==librarian3 and group==librarian2", URLDecoder.decode(g6, "UTF-8"));
      //build cql by requesting values from a non existent field in the json, should be an emptry string ""
      String g7 = new BuildCQL(r, "arr3", "group").buildCQL();
      assertEquals("", URLDecoder.decode(g7, "UTF-8"));

    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
