package org.z3950.zing.cql.cql2pgjson;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;


@RunWith(JUnitParamsRunner.class)
public class ForeignKeyGenerationIT extends DatabaseTestBase {
  private static Logger logger = Logger.getLogger(CQL2PgJSONTest.class.getName());
  private static CQL2PgJSON cql2pgJson;

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
    setupDatabase();
    runSqlFile("joinExample.sql");
  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }
  
}
