package org.folio.dbschema;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Date;

import org.folio.okapi.testing.UtilityClassTester;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

class ObjectMapperToolTest {

  @Test
  void isUtilityClass() {
    UtilityClassTester.assertUtilityClass(ObjectMapperTool.class);
  }

  @Test
  void testReadValueOK() {
    String dbJson = ResourceUtil.asString("schema.json");
    Schema dbSchema = ObjectMapperTool.readValue(dbJson, Schema.class);
    assertThat(dbSchema.getTables().get(0).getTableName(), is("item"));
  }

  @Test
  void testReadValueException() {
    Assertions.assertThrows(UncheckedIOException.class,
        () -> ObjectMapperTool.readValue("{\"foo\":true}", Schema.class));
  }

  @Test
  void canReadSchema() throws Throwable {
    String dbJson = ResourceUtil.asString("schema.json");
    Schema dbSchema = ObjectMapperTool.getMapper().readValue(dbJson, Schema.class);
    assertThat(dbSchema.getTables().get(0).getTableName(), is("item"));
    assertThat(dbSchema.getTables().get(0).getLikeIndex().get(0)
      .getArraySubfield(), is("name"));
    assertThat(dbSchema.getTables().get(0).getLikeIndex().get(0)
      .getArrayModifiers().get(0), is("languageId"));
    assertThat(dbSchema.getTables().get(0).getLikeIndex().get(0)
      .isRemoveAccents(), is(true));
    assertThat(dbSchema.getTables().get(0).getFullTextIndex().get(0)
      .isRemoveAccents(), is(false));
    assertThat(dbSchema.getScripts().get(0)
      .getSnippetPath(), is("script.sql"));
    assertThat(dbSchema.getScripts().get(0)
      .getRun(), is("before"));
    assertThat(dbSchema.getScripts().get(0)
      .getFromModuleVersion(), is("mod-foo-18.2.2"));
    assertThat(dbSchema.getScripts().get(0).isNewForThisInstall("mod-foo-18.2.1"),
      is(true));
    assertThat(dbSchema.getScripts().get(0).isNewForThisInstall("mod-foo-18.2.3"),
      is(false));
    assertThat(dbSchema.getScripts().get(0).isNewForThisInstall("18.2.1"),
      is(true));
    assertThat(dbSchema.getScripts().get(0).isNewForThisInstall("18.2.2"),
      is(false));
    assertNull(dbSchema.getScripts().get(1).getFromModuleVersion());
    assertThat(dbSchema.getScripts().get(1).isNewForThisInstall("18.2.2"),
      is(true));
    assertThat(dbSchema.getExactCount(), is(10000));
  }

  @ParameterizedTest
  @CsvSource({
    " 2024-12-31T13:14:15.789+00:00, 2024-12-31T13:14:15.789+00:00",
    " 0000-01-01T00:00:00.000+00:00, 0000-01-01T00:00:00.000+00:00",
    "+0000-01-01T00:00:00.000+00:00, 0000-01-01T00:00:00.000+00:00",
    "+0000-12-31T23:59:59.999+00:00, 0000-12-31T23:59:59.999+00:00",
  })
  void date(String input, String expected) throws Exception {
    var json = '"' + input + '"';
    var date = ObjectMapperTool.readValue(json, Date.class);
    var json2 = ObjectMapperTool.getMapper().writeValueAsString(date);
    assertThat(json2, is('"' + expected + '"'));
  }

  static class Foo {
    public String s;
    public Date dueDate;
  }

  @Test
  void foo() throws JsonProcessingException {
    var json = "{\"s\":\"a\",\"dueDate\":\"+1970-01-01T00:00:00.000+00:00\"}";
    var foo = ObjectMapperTool.readValue(json, Foo.class);
    assertThat(foo.s, is("a"));
    assertThat(foo.dueDate.toInstant(), is(Instant.parse("1970-01-01T00:00:00.000+00:00")));
    var json2 = ObjectMapperTool.getMapper().writeValueAsString(foo);
    var expected = json.replace("+1970", "1970");
    assertThat(json2, is(expected));
  }

  @Test
  void fooException() {
    var json = "{\"dueDate\": true}";
    var e = assertThrows(Exception.class, () -> ObjectMapperTool.readValue(json, Foo.class));
    assertThat(e.getMessage(), containsString("expected string containing a date"));
    assertThat(e.getMessage(), containsString("Foo[\"dueDate\"]"));
    assertThat(e.getCause(), instanceOf(MismatchedInputException.class));
  }
}
