package org.folio.rest.tools.utils;

import static org.junit.Assert.assertEquals;
import org.folio.rest.tools.client.BuildCQL;
import org.folio.rest.tools.client.Response;
import org.folio.util.PercentCodec;
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

      String g = new BuildCQL(r, "arr2[0]", "group").buildCQL();
      assertEquals("?query=" + PercentCodec.encode("group=={\"o\":{\"bbb\":\"aaa\"}}"), g);
      g = new BuildCQL(r, "arr", "group").buildCQL();
      assertEquals("?query=" + PercentCodec.encode("group==[\"librarian3\",\"librarian2\"]"), g);
      g = new BuildCQL(r, "arr[0]", "group").buildCQL();
      assertEquals("?query=" + PercentCodec.encode("group==librarian3"), g);
      g = new BuildCQL(r, "arr[*]", "group").buildCQL();
      assertEquals("?query=" + PercentCodec.encode("group==librarian3 or group==librarian2"), g);
      g = new BuildCQL(r, "arr[*]", "group", "query1").buildCQL();
      assertEquals("?query1=" + PercentCodec.encode("group==librarian3 or group==librarian2"), g);
      g = new BuildCQL(r, "arr[*]", "group", "query1", false, "and").buildCQL();
      assertEquals("&query1=" + PercentCodec.encode("group==librarian3 and group==librarian2"), g);
      //build cql by requesting values from a non existent field in the json, should be an emptry string ""
      g = new BuildCQL(r, "arr3", "group").buildCQL();
      assertEquals("", g);
      g = new BuildCQL(r, "arr[*]", "group", "query1", false, "and", "=").buildCQL();
      assertEquals("&query1=" + PercentCodec.encode("group=librarian3 and group=librarian2"), g);
  }

}
