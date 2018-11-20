package org.folio.rest.tools.parser;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
// In Eclipse remove JUnit 5 from the build path to avoid this error:
// java.lang.SecurityException: class "org.hamcrest.Matchers"'s signer information
// does not match signer information of other classes in the same package
import static org.hamcrest.Matchers.contains;

import java.io.IOException;
import java.util.List;

import org.folio.rest.tools.parser.JsonPathParser.Pairs;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonPathParserTest {
  @Test
  void userdata() {
    JsonObject j22 = new JsonObject(ResourceUtil.asString("userdata.json"));
    JsonPathParser jp2 = new JsonPathParser(j22, true);
    assertThat(jp2.getValueAt("personal.preferredContact.desc.type", false, false), is("string"));
    assertThat(jp2.getValueAt("personals.preferredContact.desc.type", false, false), is(nullValue()));
    assertThat(jp2.getValueAt("personal.properties.preferredContact.properties.desc.type", false, false), is(nullValue()));
    assertThat(jp2.getAbsolutePaths("personal.preferredContact.desc.type").get(0).toString(),
                                 is("personal.preferredContact.desc.type"));
    assertThat(jp2.getValueAndParentPair("personal.preferredContact.desc.type").getRequestedValue(), is("string"));
    assertThat(jp2.getValueAndParentPair("personal2.preferredContact.desc.type"), is(nullValue()));
  }

  @ParameterizedTest
  @ValueSource(strings = {
       "c.arr[0].a2.'aaa.ccc' | "+
      "[c.arr[0].a2.'aaa.ccc']",

       "c.arr[*].a2.'aaa.ccc' | " +
      "[c.arr[0].a2.'aaa.ccc', " +
       "c.arr[2].a2.'aaa.ccc']",

       "c.arr[*].a2.arr2[*].arr3[*] | " +
      "[c.arr[0].a2.arr2[2].arr3[0], " +
       "c.arr[0].a2.arr2[2].arr3[1]]",

       "c.arr[*].a2.arr2[*].arr3[*].a32 | " +
      "[c.arr[0].a2.arr2[2].arr3[0].a32, " +
       "c.arr[0].a2.arr2[2].arr3[1].a32]",
  })
  void getAbsolutePaths(String test) throws IOException {
    String [] param = test.split("\\|");
    getAbsolutePaths(param[0].trim(), param[1].trim());
  }

  void getAbsolutePaths(String path, String expected) {
    List<StringBuilder> list = pathTestParser().getAbsolutePaths(path);
    assertThat(list.toString(), is(expected));
  }

  private JsonPathParser pathTestParser() {
    return new JsonPathParser(new JsonObject(ResourceUtil.asString("pathTest.json")));
  }

  @ParameterizedTest
  @CsvSource({
    "c.arr[1].a2.arr2[1]",
    "c.arr[3].a2.arr2[2].arr3[1]",
    "c.arr[*].a2.arr2[*].arr3[*].a32",
  })
  void getValueAndParentPairNull(String path) {
    assertThat(pathTestParser().getValueAndParentPair(path), is(nullValue()));
  }

  @ParameterizedTest
  @CsvSource({
      "c.arr[0].a2.'aaa.ccc', aaa.bbb", //fix
      "c.a1, 1",
  })
  void getValueAndParentPair(String path, String expectedValue) {
    assertThat(pathTestParser().getValueAndParentPair(path).requestedValue, is(expectedValue));
  }

  @ParameterizedTest
  @CsvSource({
    "c.arr[*].a2.'aaa.ccc'",
    "c.arr[*].a2",

  })
  void getValueAtWithParentIsNull(String path) {
    assertThat(pathTestParser().getValueAt(path, true, false), is(nullValue()));
  }

  @ParameterizedTest
  @CsvSource({
    "c.arr[0].a2",
  })
  void getValueAtWithParent(String path) {
    assertThat(pathTestParser().getValueAt(path, true, false), is(instanceOf(Pairs.class)));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "c.arr[*].a2.arr2[*].arr3[*] | [{\"a32\":\"5\"}, {\"a32\":\"6\"}]",
    "c.arr[*].a2.arr2[*] | [{\"a30\":\"1\"}, {\"a30\":\"2\"}, {\"arr3\":[{\"a32\":\"5\"},{\"a32\":\"6\"}]}, {\"a31\":\"3\"}, {\"a31\":\"4\"}]",
  })
  void getValueAt(String test) {
    String [] param = test.split("\\|");
    String path = param[0].trim();
    String expected = param[1].trim();
    assertThat(pathTestParser().getValueAt(path, true, false).toString(), is(expected));
  }

  @SuppressWarnings("unchecked")
  private List<String> list(JsonPathParser jsonPathParser, String path) {
    return (List<String>) jsonPathParser.getValueAt(path);
  }

  @Test
  void setValue0and2() {
    JsonPathParser parser = pathTestParser();
    assertThat(list(parser, "c.arr[*].a2.'aaa.ccc'"), contains("aaa.bbb", null, "aaa.bbb"));
    parser.setValueAt("c.arr[0].a2.'aaa.ccc'", "aaa.ccc");
    parser.setValueAt("c.arr[2].a2.'aaa.ccc'", "aaa.ddd");
    assertThat(list(parser, "c.arr[*].a2.'aaa.ccc'"), contains("aaa.ccc", null, "aaa.ddd"));
  }

  @Test
  void setValue0() {
    JsonPathParser parser = pathTestParser();
    String before = parser.getValueAt("c.arr[*].a2.arr2[*]").toString();
    parser.setValueAt("c.arr[0].a2.arr2[0]", "yyy");
    String after  = parser.getValueAt("c.arr[*].a2.arr2[*]").toString();
    assertThat(before, is("[{\"a30\":\"1\"}, {\"a30\":\"2\"}, {\"arr3\":[{\"a32\":\"5\"},{\"a32\":\"6\"}]}, {\"a31\":\"3\"}, {\"a31\":\"4\"}]"));
    assertThat(after,  is(            "[yyy, {\"a30\":\"2\"}, {\"arr3\":[{\"a32\":\"5\"},{\"a32\":\"6\"}]}, {\"a31\":\"3\"}, {\"a31\":\"4\"}]"));
  }

  @Test
  void setValueAaaCcc() {
    JsonPathParser parser = pathTestParser();
    assertThat(parser.getValueAt("c.arr[1].a2.'aaa.ccc'"), is(nullValue()));
    parser.setValueAt(           "c.arr[1].a2.'aaa.ccc'",     "aaa.ddd");
    assertThat(parser.getValueAt("c.arr[1].a2.'aaa.ccc'"), is("aaa.ddd"));
  }

  @Test
  void getArr3() {
    assertThat(((JsonObject)pathTestParser().getValueAt("c.arr[0].a2.arr2[2]")).getJsonArray("arr3").toString(),
        is("[{\"a32\":\"5\"},{\"a32\":\"6\"}]"));
  }

  @Test
  void setA2() {
    JsonPathParser parser = pathTestParser();
    parser.setValueAt("c.arr[*]", new JsonObject());
    JsonObject jsonObject = new JsonObject("{\"xqq\":\"xaa\"}");
    parser.setValueAt("c.arr[1].a2", jsonObject);
    assertThat(parser.getValueAt("c.arr[1].a2"), is(jsonObject));
    assertThat(parser.getValueAt("c.arr[1]").toString(), is("{\"ignore\":\"true\",\"a2\":{\"xqq\":\"xaa\"}}"));
  }

  @Test
  void setArr1() {
    JsonPathParser parser = pathTestParser();
    JsonArray jsonArray = new JsonArray("[{\"xqq\":\"xaa\"}]");
    parser.setValueAt("c.arr[1]", jsonArray);
    assertThat(parser.getValueAt("c.arr[1]"), is(jsonArray));
  }

  @Test
  void setArr() {
    JsonPathParser parser = pathTestParser();
    JsonArray jsonArray = new JsonArray("[{\"xqq\":\"xaa\"}]");
    parser.setValueAt("c.arr", jsonArray);
    assertThat(parser.getValueAt("c.arr"), is(jsonArray));
  }

  @Test
  void setC() {
    JsonPathParser parser = pathTestParser();
    JsonObject jsonObject = new JsonObject("{\"xqq\":\"xaa\"}");
    parser.setValueAt("c", jsonObject);
    assertThat(parser.getValueAt("c"), is(jsonObject));
  }
}
