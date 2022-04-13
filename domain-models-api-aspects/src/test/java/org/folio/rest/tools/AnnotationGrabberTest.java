package org.folio.rest.tools;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

public class AnnotationGrabberTest {

  @Test
  public void generateMappings() throws Exception {
    JsonObject mappings = AnnotationGrabber.generateMappings(null);
    JsonObject unittests = mappings.getJsonObject("unittests");
    assertThat(unittests.getString("class"), is("org.folio.rest.jaxrs.resource.TestResource"));
    JsonArray books = unittests.getJsonArray("^unittestsbooks/?$");
    JsonObject book0 = books.getJsonObject(0);
    assertThat(book0.getString("function"), is("getRmbtestsBooks"));
    JsonObject params = book0.getJsonObject("params");
    JsonObject param0 = params.getJsonObject("0");
    assertThat(param0.getString("value"), is("author"));
    assertThat(param0.getString("type"), is("java.lang.String"));
  }
}
