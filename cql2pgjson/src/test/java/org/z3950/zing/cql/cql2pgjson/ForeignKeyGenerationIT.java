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
  public void populateTestData() {
    //insert stuff into table A
    //"11111111-1111-1111-1111-111111111111", {"id" : "11111111-1111-1111-1111-111111111111", "name": "test1  }
    //"22222222-2222-2222-2222-222222222222", {"id" : "22222222-2222-2222-2222-222222222222", "name": "test2  }
    //"33333333-3333-3333-3333-333333333333", {"id" : "33333333-3333-3333-3333-333333333333", "name": "test3  }
    runSqlStatement("Insert into tablea (id, jsonb) VALUES ('A1111111-1111-1111-1111-111111111111', json_build_object('id' , 'A1111111-1111-1111-1111-111111111111', 'name', 'test1') )");
    runSqlStatement("Insert into tablea (id, jsonb) VALUES ('A2222222-2222-2222-2222-222222222222', json_build_object('id' , 'A2222222-2222-2222-2222-222222222222', 'name', 'test2') )");
    runSqlStatement("Insert into tablea (id, jsonb) VALUES ('A3333333-3333-3333-3333-333333333333', json_build_object('blah' , 'A3333333-3333-3333-3333-333333333333', 'name', 'test3') )");
    
    //insert stuff into table b
    runSqlStatement("Insert into tableb (id, jsonb) VALUES ('B1111111-1111-1111-1111-111111111111', json_build_object('id' , 'B1111111-1111-1111-1111-111111111111', 'tableb_data','C111111C-1111-1111-1111-C1111111111C', 'tableaId' , 'A22222222-2222-2222-2222-222222222222' ))");
    runSqlStatement("Insert into tableb (id, jsonb) VALUES ('B2222222-2222-2222-2222-222222222222', json_build_object('id' , 'B2222222-2222-2222-2222-222222222222', 'tableb_data','C222222C-2222-2222-2222-C22222222222C', 'tableaId' , 'A1111111-1111-1111-1111-111111111111' ))");
    
    
   
  }
  @Test
  public void ForeignKeySearch() throws FieldException, QueryValidationException, ServerChoiceIndexesException {
    populateTestData();
    CQL2PgJSON cql2pgJson = new CQL2PgJSON("tablea.json" );
    cql2pgJson.setDbSchemaPath("templates/db_scripts/joinExample_schema.json");
    String sql = cql2pgJson.toSql("tableb.tableb_data == C111111C-1111-1111-1111-C1111111111C").toString();
    // default pkColumnName is id without underscore
    runSqlStatement("select jsonb from tablea " + sql);
    //assertEquals(" ( SELECT tableb.jsonb->>'tableb_data' from tableb WHERE (lower(f_unaccent(tablea.jsonb->>'id'::text)) = lower(f_unaccent(tableb.jsonb->>'tableb_data'::text)))) LIKE lower(f_unaccent(11111111-1111-1111-1111-111111111111))", sql);
  }
  
}
