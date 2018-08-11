package org.folio.rest.tools;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.junit.jupiter.api.Test;

public class AnnotationGrabberTest {
  private static final Logger log = LoggerFactory.getLogger(AnnotationGrabberTest.class);

  @Test
  void generateMappings() throws Exception {
    JsonObject mappings = AnnotationGrabber.generateMappings();
    log.debug(mappings.encodePrettily());
    JsonObject unittests = mappings.getJsonObject("^unittests");
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
