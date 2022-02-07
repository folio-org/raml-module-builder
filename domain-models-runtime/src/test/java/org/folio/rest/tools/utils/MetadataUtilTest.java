package org.folio.rest.tools.utils;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataUtilTest {
  final static String token = "header." +
      Base64.getEncoder().encodeToString("{\"user_id\": \"bar\"}".getBytes()) +
      ".signature";
  Date dateBefore;

  @BeforeEach
  void nowBefore() {
    dateBefore = new Date();
  }

  void assertNow(Date now) {
    Date dateAfter = new Date();
    assertThat(now, both(greaterThanOrEqualTo(dateBefore)).and(lessThanOrEqualTo(dateAfter)));
  }

  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(MetadataUtil.class);
  }

  Map<String, String> headers(String ...strings) {
    Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (int i = 0; i < strings.length; i += 2) {
      headers.put(strings[i], strings[i + 1]);
    }
    return headers;
  }

  @Test
  void createMetadata() {
    Metadata metadata = MetadataUtil.createMetadata(headers());
    assertThat(metadata.getCreatedByUserId(), is(nullValue()));
    assertThat(metadata.getUpdatedByUserId(), is(nullValue()));
    assertNow(metadata.getCreatedDate());
    assertNow(metadata.getUpdatedDate());
  }

  @Test
  void createMetadataFromUserId() {
    Metadata metadata = MetadataUtil.createMetadata(headers(
        RestVerticle.OKAPI_HEADER_TOKEN, token,
        RestVerticle.OKAPI_USERID_HEADER, "foo"));
    assertThat(metadata.getCreatedByUserId(), is("foo"));
    assertThat(metadata.getUpdatedByUserId(), is("foo"));
    assertNow(metadata.getCreatedDate());
    assertNow(metadata.getUpdatedDate());
  }

  @Test
  void createMetadataFromToken() {
    Metadata metadata = MetadataUtil.createMetadata(headers(
        RestVerticle.OKAPI_HEADER_TOKEN, token));
    assertThat(metadata.getCreatedByUserId(), is("bar"));
    assertThat(metadata.getUpdatedByUserId(), is("bar"));
    assertNow(metadata.getCreatedDate());
    assertNow(metadata.getUpdatedDate());
  }

  @Test
  void populateMetadataNoop() throws Exception {
    String uuid = UUID.randomUUID().toString();
    User user = new User().withId(uuid);
    MetadataUtil.populateMetadata(user, headers());
    assertThat(user.getId(), is(uuid));
  }

  class WrongMethods {
    public void setMetadata(String s) {
    }
    public void setMetadata(Metadata metadata, String x) {
    }
  }

  @Test
  void populateMetadataNoopWhenWrongMethods() {
    assertDoesNotThrow(() ->
    MetadataUtil.populateMetadata(new WrongMethods(), headers()));
  }

  @Test
  void populateMetadataListNoopWhenWrongMethods() {
    List<WrongMethods> list = Arrays.asList(null, new WrongMethods());
    assertDoesNotThrow(() ->
    MetadataUtil.populateMetadata(list, headers()));
  }

  class Foo {
    Metadata metadata;
    public void setMetadata(String s) {
    }
    public void setMetadata(Metadata metadata, String x) {
    }
    public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
    }
  }

  @Test
  void populateMetadata() throws Exception {
    Foo foo = new Foo();
    MetadataUtil.populateMetadata(foo, headers(RestVerticle.OKAPI_USERID_HEADER, "Boe"));
    assertThat(foo.metadata.getCreatedByUserId(), is("Boe"));
    assertThat(foo.metadata.getCreatedDate(), is(notNullValue()));
  }

  @Test
  void populateNullWithMetadata() {
    assertDoesNotThrow(() ->
    MetadataUtil.populateMetadata((Foo) null, headers(RestVerticle.OKAPI_USERID_HEADER, "Boe")));
  }

  class Bar {
    public void setMetadata(Metadata metadata) {
      throw new RuntimeException();
    }
  }

  @Test
  void populateMetadataSetterThrowsException() throws Exception {
    assertThrows(InvocationTargetException.class, () ->
    MetadataUtil.populateMetadata(new Bar(), headers(RestVerticle.OKAPI_USERID_HEADER, "Boe")));
  }

  @Test
  void populateMetadataList() throws ReflectiveOperationException {
    List<Foo> entities = Arrays.asList(null, new Foo(), null, new Foo());
    MetadataUtil.populateMetadata(entities, headers(RestVerticle.OKAPI_USERID_HEADER, "Fox"));
    assertThat(entities.get(1).metadata.getCreatedByUserId(), is("Fox"));
    assertThat(entities.get(3).metadata.getCreatedByUserId(), is("Fox"));
    assertThat(entities.get(1).metadata.getCreatedDate(), is(notNullValue()));
    assertThat(entities.get(3).metadata.getCreatedDate(), is(notNullValue()));
  }

  @Test
  void populateMetadataListNull() {
    assertDoesNotThrow(() ->
    MetadataUtil.populateMetadata((List<Foo>)null, headers(RestVerticle.OKAPI_USERID_HEADER, "Fox")));
  }

  @Test
  void populateMetadataListEmpty() {
    assertDoesNotThrow(() ->
    MetadataUtil.populateMetadata(new ArrayList<Foo>(), headers(RestVerticle.OKAPI_USERID_HEADER, "Fox")));
  }
}
