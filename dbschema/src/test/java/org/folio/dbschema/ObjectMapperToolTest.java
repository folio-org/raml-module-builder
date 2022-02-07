package org.folio.dbschema;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.UncheckedIOException;
import org.folio.okapi.testing.UtilityClassTester;
import org.folio.util.ResourceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
