package org.z3950.zing.cql.cql2pgjsoncli;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class TestCLI {

  private static Logger logger = Logger.getLogger(TestCLI.class.getName());

  int exitStatus;
  String instanceSchemaPath;
  String holdingSchemaPath;
  String dbSchemaPath;

  @Before
  public void setup() throws URISyntaxException {
    exitStatus = 0;
    CQL2PGCLIMain.exit = status -> exitStatus = status;
    dbSchemaPath = Paths.get("./dbschema.json").toString();
  }

  private void main(String arguments) {
    CQL2PGCLIMain.main(arguments.split(" "));
  }

  @Test
  public void testMainWithoutArguments() throws Exception {
    main("");
    assertEquals(1, exitStatus);
  }

  @Test
  public void testMain() throws Exception {
    main("-t instance cql.allRecords=1");
    assertEquals(0, exitStatus);
  }

  private String handleOptions(String[] arguments) throws Exception {
    return CQL2PGCLIMain.handleOptions(arguments);
  }

  @Test
  public void testCLIWithNoSchemaOrDBSchema() throws FieldException, IOException,
      QueryValidationException, ParseException {
    String cql = "holdingsRecords.permanentLocationId=\"fcd64ce1-6995-48f0-840e-89ffa2\"";
    String[] args = new String[] {"-t", "instance", "-f", "jsonb", cql };
    String fullFieldName = "instance.jsonb";
    CQL2PgJSON cql2pgjson = new CQL2PgJSON(fullFieldName);
    String output = CQL2PGCLIMain.parseCQL(cql2pgjson, "instance", cql);
    String cli_output = CQL2PGCLIMain.handleOptions(args);
    logger.info(output);
    logger.info(cli_output);
    assertNotNull(output);
    assertEquals(output, cli_output);
  }

  @Test
  public void testCLIWithDBSchema() throws FieldException, IOException,
      ParseException, QueryValidationException {
    String cql = "hrid=\"fcd64ce1-6995-48f0-840e-89ffa2\"";
    String[] args = new String[] {"-t", "instance", "-f", "jsonb", "-b", dbSchemaPath, cql };
    String fullFieldName = "instance.jsonb";
    CQL2PgJSON cql2pgjson = new CQL2PgJSON(fullFieldName);
    cql2pgjson.setDbSchemaPath(dbSchemaPath);
    String output = CQL2PGCLIMain.parseCQL(cql2pgjson, "instance", cql);
    String cli_output = CQL2PGCLIMain.handleOptions(args);
    assertNotNull(output);
    logger.info(output);
    logger.info(cli_output);
    assertEquals(output, cli_output);
  }

  void testCLI(String cql, String expectedSql) throws Exception {
      String[] args = new String[] { "-t", "instance", cql };
      String actualSql = handleOptions(args);
      assertEquals(expectedSql, actualSql);
  }

  @Test
  public void testCLIAllRecords() throws Exception {
    testCLI("cql.allRecords=1",
        "select * from instance where true");
  }

  @Test
  public void testCLIAllRecordsSorted() throws Exception {
    testCLI("cql.allRecords=1 sortBy title",
        "select * from instance where true order by lower(f_unaccent(instance.jsonb->>'title'))");
  }

  @Test
  public void testCLIName() throws Exception {
    String cql = "title=foo";
    String[] args = new String[] { "-t", "instance", cql };
    String sql = handleOptions(args);
    assertTrue(sql.contains("instance.jsonb->>'title'"));
    assertTrue(sql.contains("'foo'"));
  }

  @Test(expected = QueryValidationException.class)
  public void testCLIParseException() throws Exception {
    testCLI("=", null);
  }
}
