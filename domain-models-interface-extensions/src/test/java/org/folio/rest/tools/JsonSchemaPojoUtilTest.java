package org.folio.rest.tools;

public class JsonSchemaPojoUtilTest {

/*  private static final String FILE_PATH = "src/test/resources/FacetValue.java";
  private static final String RAML_PATH = "src/test/resources/schemas/jobs.raml";
  private static final File RAML_FILE = new File(RAML_PATH);

  @Test
  public void jsonFields2Pojo() throws Exception {
    Map<Object, Object> jsonField2PojoMap = JsonSchemaPojoUtil.jsonFields2Pojo(FILE_PATH);
    assertThat(jsonField2PojoMap.size(), is(2));
    assertThat(jsonField2PojoMap.get("count"), is("Integer"));
    assertThat(jsonField2PojoMap.get("value"), is("Object"));
  }

  @Test
  public void raml() throws Exception {
    List<GlobalSchema> schemas = JsonSchemaPojoUtil.getSchemasFromRaml(RAML_FILE);
    assertThat(schemas.size(), is(2));
    assertThat(schemas.get(0).key(), is("jobs.schema"));
    assertThat(schemas.get(1).key(), is("job.schema"));
  }

  private JsonObject jobSchema() {
    List<GlobalSchema> schemas = JsonSchemaPojoUtil.getSchemasFromRaml(RAML_FILE);
    assertThat(schemas.get(1).key(), is("job.schema"));
    return new JsonObject(schemas.get(1).value().value());
  }

  @Test
  public void readonly() {
    List<String> paths = JsonSchemaPojoUtil.getFieldsInSchemaWithType(jobSchema(), "readonly", true);
    assertThat(paths.size(), is(2));
    assertThat(paths, hasItems("creator", "creator_date"));
  }

  @Test
  public void getAllFieldsInSchemaNull() {
    assertThat(JsonSchemaPojoUtil.getAllFieldsInSchema(null), nullValue());
  }

  @Test
  public void getAllFieldsInSchema() {
    List<String> fields = JsonSchemaPojoUtil.getAllFieldsInSchema(jobSchema());
    assertThat(fields.size(), is(5));
    assertThat(fields, hasItems("_id", "name", "module", "creator", "creator_date"));
  }*/
}
