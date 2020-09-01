package org.folio.cql2pgjson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Test;

public class CompoundIndexTest {

  @Test
  public void multiFieldNamesNonUniqueIndex() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("keys == x").toString();
    String expected = "WHERE CASE WHEN length(lower(f_unaccent('x'))) <= 600 "
        + "THEN left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2'))),600) LIKE lower(f_unaccent('x')) "
        + "ELSE left(lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2'))),600) LIKE left(lower(f_unaccent('x')),600) "
        + "AND lower(f_unaccent(concat_space_sql(tablea.jsonb->>'key1' , tablea.jsonb->>'key2'))) LIKE lower(f_unaccent('x')) END";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldNamesUniqueIndex() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("barcode == y").toString();
    String expected = "WHERE lower(f_unaccent(concat_space_sql(tablea.jsonb->>'department' , tablea.jsonb->>'staffnumber'))) LIKE lower(f_unaccent('y'))";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldNamesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("fullname == \"John Smith\"").toString();
    String expected = "WHERE lower(concat_space_sql(tablea.jsonb->>'firstName' , tablea.jsonb->>'lastName')) LIKE lower('John Smith')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldNamesFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = \"John Smith\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(tablea.jsonb->>'field1' , tablea.jsonb->>'field2')) @@ tsquery_phrase('John Smith')";
    assertThat(sql, is(expected));
  }

  @Test
  public void SQLExpressionGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("address == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(jsonb->>'city', jsonb->>'state')) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected));
  }

  @Test
  public void SQLExpressionQueryGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("cityWithoutVocals == Boston").toString();
    String expected = "translate(jsonb->>'city', 'aeiouAEIOU', '') LIKE translate('Boston', 'aeiouAEIOU', '')";
    assertThat(sql, containsString(expected));
  }

  @Test
  public void SQLExpressionFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfield = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(lower(concat_space_sql(jsonb->>'field1', jsonb->>'field2'))) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void SQLExpressionQueryFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tableb");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("stateReverse = MA").toString();
    String expected = "WHERE get_tsvector(reverse(jsonb->>'state')) @@ tsquery_phrase(reverse('MA'))";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesSpacesFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecftindex = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesSpacesGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablec");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tablecginindex == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(tablec.jsonb->>'firstName' , tablec.jsonb->>'lastName')) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected ));
  }

  @Test
  public void multiFieldnamesMultipartFT() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledftindex = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state')) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesFTStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfieldstar = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') , concat_array_object_values(tablea.jsonb->'field2','state'))) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesFTDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstar = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') , concat_array_object_values(tablea.jsonb->'field3'->'info','state'))) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesFTDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ftfielddotstarplain = \"Boston MA\"").toString();
    String expected = "WHERE get_tsvector(concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') , concat_array_object(tablea.jsonb->'field3'->'data'))) @@ tsquery_phrase('Boston MA')";
    assertThat(sql, is(expected));
  }

  @Test
  public void multiFieldnamesGINStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfieldstar == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field1','city') ,"
        + " concat_array_object_values(tablea.jsonb->'field2','state'))) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected));
  }

  @Test
  public void multiFieldnamesGINDotStar() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstar == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(concat_array_object_values(tablea.jsonb->'field3'->'info','city') ,"
        + " concat_array_object_values(tablea.jsonb->'field3'->'info','state'))) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected));
  }

  @Test
  public void multiFieldnamesGINDotStarPlain() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("ginfielddotstarplain == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(concat_array_object(tablea.jsonb->'field3'->'info') ,"
        + " concat_array_object(tablea.jsonb->'field3'->'data'))) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected));
  }

  @Test
  public void multiFieldnamesMultipartGIN() throws Exception {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tabled");
    cql2pgJson.setDbSchemaPath("templates/db_scripts/compoundIndex.json");
    String sql = cql2pgJson.toSql("tabledginindex == \"Boston MA\"").toString();
    String expected = "lower(concat_space_sql(tabled.jsonb->'proxy'->'personal'->>'city' , tabled.jsonb->'proxy'->'personal'->>'state')) LIKE lower('Boston MA')";
    assertThat(sql, containsString(expected));
  }
}
