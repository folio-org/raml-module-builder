package org.folio.cql2pgjson;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.runner.RunWith;
import org.z3950.zing.cql.ModifierSet;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.cql2pgjson.exception.CQL2PgJSONException;
import org.folio.cql2pgjson.exception.CQLFeatureUnsupportedException;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.exception.ServerChoiceIndexesException;
import org.folio.cql2pgjson.model.CqlMasking;
import org.folio.cql2pgjson.model.CqlModifiers;
import org.folio.cql2pgjson.model.SqlSelect;
import org.folio.dbschema.Table;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class CQL2PgJSONTest extends DatabaseTestBase {
  private static  CQL2PgJSON cql2pgjsonRespectCase;
  private static CQL2PgJSON cql2pgjsonRespectAccents;
  private static CQL2PgJSON cql2pgjsonRespectBoth;
  private static Logger logger = LogManager.getLogger(CQL2PgJSONTest.class);
  private static CQL2PgJSON cql2pgJson;

  /**
   * whether to reject any where-clause that contains lower.
   * set this to true for full text queries. tsvector automatically lower cases and
   * we don't want an additional lower() invocation because that prevents using the index.
   */
  private boolean rejectLower;

  @BeforeClass
  public static void runOnceBeforeClass() throws Exception {
    setupDatabase();
    runSqlFile("users.sql");
    runGeneralFunctions();
    cql2pgJson = new CQL2PgJSON("users.user_data", Arrays.asList("name", "email"));
    cql2pgjsonRespectCase = new CQL2PgJSON("users.user_data", Arrays.asList("name","email"));
    cql2pgjsonRespectCase.setDbSchemaPath("./templates/db_scripts/schemaWithRespectCase.json");
    cql2pgjsonRespectAccents = new CQL2PgJSON("users.user_data", Arrays.asList("name","email"));
    cql2pgjsonRespectAccents.setDbSchemaPath("./templates/db_scripts/schemaWithRespectAccents.json");
    cql2pgjsonRespectBoth = new CQL2PgJSON("users.user_data", Arrays.asList("name","email"));
    cql2pgjsonRespectBoth.setDbSchemaPath("./templates/db_scripts/schemaWithRespectBoth.json");
  }

  @AfterClass
  public static void runOnceAfterClass() {
    closeDatabase();
  }

  @Before
  public void before() {
    rejectLower = false;
  }

  public void select(CQL2PgJSON aCql2pgJson, String sqlFile, String testcase) {
    int hash = testcase.indexOf('#');
    assertTrue("hash character in testcase", hash >= 0);
    String cql = testcase.substring(0, hash).trim();
    String expectedNames = testcase.substring(hash + 1).trim();
    select(aCql2pgJson, sqlFile, cql, expectedNames);
  }

  /**
   * @param expectedNames the semicolon+space separated list of expected names, or -- if there should
   *          be an exception -- the expected substring of the error message prepended by an exclamation mark.
   */
  public void select(CQL2PgJSON aCql2pgJson, String sqlFile, String cql, String expectedNames) {

    if (! cql.contains(" sortBy ")) {
      cql += " sortBy name";
    }
    String sql = null;
    try {
      String blob = "user_data";
      String tablename = "users";
      // Dirty hack, the tests had the table name and blob name hard coded,
      // but the instances test uses different ones. Some day we may clean
      // this mess up
      if ("instances.jsonb".equals(aCql2pgJson.getjsonField())) {
        blob = "jsonb";
        tablename = "instances";
      }
      SqlSelect sqlSelect = aCql2pgJson.toSql(cql);
      if (rejectLower) {
        assertThat(sqlSelect.getWhere().toLowerCase(Locale.ROOT), not(containsString("lower")));
      }
      sql = "select " + blob + "->'name' from " + tablename + " " + sqlSelect;
      logger.info("select: CQL --> SQL: " + cql + " --> " + sql);
      runSqlFile(sqlFile);
      logger.debug("select: sqlfile done");
      String actualNames = "";
      try ( Statement statement = conn.createStatement();
            ResultSet result = statement.executeQuery(sql) ) {

        while (result.next()) {
          if (! actualNames.isEmpty()) {
            actualNames += "; ";
          }
          actualNames += result.getString(1).replace("\"", "");
        }
      }
      if (! expectedNames.equals(actualNames)) {
        logger.debug("select: Test FAILURE on " + cql + "#" + expectedNames);
      }
      logger.debug("select: Got names [" + actualNames + "], expected [" + expectedNames + "]");
      assertEquals("CQL: " + cql + ", SQL: " + sql, expectedNames, actualNames);
    } catch (QueryValidationException | SQLException e) {
      logger.debug("select: " + e.getClass().getSimpleName()
        + " for query " + cql + " : " + e.getMessage());
      if (! expectedNames.startsWith("!")) {
        throw new RuntimeException(sql != null ? sql : cql, e);
      }
      assertThat(e.toString(), containsString(expectedNames.substring(1).trim()));
    }
    logger.debug("select: done with " + cql);
  }

  public void select(String sqlFile, String testcase) {
    select(cql2pgJson, sqlFile, testcase);
  }

  public void select(String testcase) {
    select(cql2pgJson, "jo-ka-lea.sql", testcase);
  }

  public void select(CQL2PgJSON aCql2pgJson, String testcase) {
    select(aCql2pgJson, "jo-ka-lea.sql", testcase);
  }

  /**
   * Invoke CQL2PgJSON.cql2pgJson(cql) expecting an exception.
   * @param cql  the cql expression that should trigger the exception
   * @param clazz  the expected class of the exception
   * @param contains  the expected strings of the exception message
   * @throws RuntimeException  if an exception was thrown that is not an instance of clazz
   */
  public void cql2pgJsonException(String cql,
      Class<? extends Exception> clazz, String ... contains) throws RuntimeException {
    try {
      CQL2PgJSON cql2pgJson = new CQL2PgJSON("users.user_data", Arrays.asList("name", "email"));
      cql2pgJsonException(cql2pgJson, cql, clazz, contains);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Invoke CQL2PgJSON.cql2pgJson(cql) expecting an exception.
   * @param cql2pgJson  the CQL2PgJSON to use
   * @param cql  the cql expression that should trigger the exception
   * @param clazz  the expected class of the exception
   * @param contains  the expected strings of the exception message
   * @throws RuntimeException  if an exception was thrown that is not an instance of clazz
   */
  public void cql2pgJsonException(CQL2PgJSON cql2pgJson, String cql,
      Class<? extends Exception> clazz, String ... contains) throws RuntimeException {
    try {
      cql2pgJson.toSql(cql).getWhere();
    } catch (Throwable e) {
      if (!clazz.isInstance(e)) {
        logger.debug("Wrong exception. Expected " + clazz + ". " + "but got " + e);
        throw new RuntimeException(e);
      }
      for (String s : contains) {
        assertTrue("Expect exception message containing '" + s + "': " + e.getMessage(),
            e.getMessage().toLowerCase(Locale.ROOT).contains(s.toLowerCase(Locale.ROOT)));
      }
      return;
    }
    fail("Exception " + clazz + " expected, but no exception thrown");
  }

  @Test
  @Parameters({
    "name=Long                      # Lea Long",
    "address.zip=2791               # Lea Long",
    "\"Lea Long\"                   # Lea Long",
    "\"Long Lea\"                   #",
    "Long                           # Lea Long",
    "Lon                            #",
    "ong                            #",
    "jo@example.com                 # Jo Jane",
    "email=\"com example\"          #",
    "email==example.com             #",
    "email<>example.com             # Jo Jane; Ka Keller; Lea Long",
    "email==ka@example.com          # Ka Keller",
    "name == \"Lea Long\"           # Lea Long",
    "name <> \"Lea Long\"           # Jo Jane; Ka Keller",
    "name=\"\"                      # Jo Jane; Ka Keller; Lea Long",
    "name=\"*\"                     # Jo Jane; Ka Keller; Lea Long",
    "name=*                         # Jo Jane; Ka Keller; Lea Long",
    "email=\"\"                     # Jo Jane; Ka Keller; Lea Long",
    "email=\"*\"                    # Jo Jane; Ka Keller; Lea Long",
    "email=*                        # Jo Jane; Ka Keller; Lea Long",
    "alternateEmail==\"ffgffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff@example.com\" # Ka Keller",
    "alternateEmail<>\"ffgffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff@example.com\" # Jo Jane; Lea Long",
    "alternateEmail==\"ffffffffffffgffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff@example.com\" #",
    "alternateEmail<>\"ffffffffffffgffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff@example.com\" # Jo Jane; Ka Keller; Lea Long",
    // 603 x "f" + "fffffffffffg@example.com"
    "alternateEmail==\"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffg@example.com\" #",
    "alternateEmail<>\"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffg@example.com\" # Jo Jane; Ka Keller; Lea Long",
    // 603 x "f" + "gfffffffffff@example.com"
    "alternateEmail==\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" # Jo Jane",
    "alternateEmail<>\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" # Ka Keller; Lea Long",
    // 603 x "f" + "hfffffffffff@example.com"
    "alternateEmail==\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Lea Long",
    "alternateEmail<>\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Jo Jane; Ka Keller",
  })
  public void basic(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "name all \"\"                               # Jo Jane; Ka Keller; Lea Long",
    "name all Lea                                # Lea Long",
    "name all Long                               # Lea Long",
    "name all \"Lea Long\"                       # Lea Long",
    "name all \"Long Lea\"                       # Lea Long",
    "name all \"FooBar\"                         #",
  })
  public void all(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "name any \"\"                               # Jo Jane; Ka Keller; Lea Long",
    "name any Lea                                # Lea Long",
    "name any Long                               # Lea Long",
    "name any \"Lea Long\"                       # Lea Long",
    "name any \"Long Lea\"                       # Lea Long",
    "name any \"Lea FooBar\"                     # Lea Long",
    "name any \"FooBar Long\"                    # Lea Long",
  })
  public void any(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "name adj \"\"                               # Jo Jane; Ka Keller; Lea Long",
    "name adj Lea                                # Lea Long",
    "name adj Long                               # Lea Long",
    "name adj \"Lea Long\"                       # Lea Long",
    "name adj \"Long Lea\"                       #",
  })
  public void adj(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({ // Can not to left trucn on names, it is fulltext
    "name=ja*                                   # Jo Jane",
    "name=lo*                                   # Lea Long",
    "              email=k*                     # Ka Keller",
    "                           address.zip=2*  # Jo Jane; Lea Long",
    "name=lo* and  email=*                      # Lea Long",
    "name=lo* or   email=k*                     # Ka Keller; Lea Long",
    "name=ja* not  email=k*                     # Jo Jane",
    "name=lo* and  email=* or  address.zip=2*   # Jo Jane; Lea Long",
    "name=lo* and (email=* or  address.zip=0*)  # Lea Long",
    "name=lo* or   email=k* and address.zip=1*  # Ka Keller",
    "name=lo* or  (email=* and address.zip=1*)  # Ka Keller; Lea Long",
    "name=lo* not (email=* or  address.zip=0*)  #",
    "name=lo* or  (email=l* not address.zip=0*) # Lea Long",
    "\"lea l*\"                                 # Lea Long",
    "\"long example\"                           #",  // no match because "long" from name and "example" from email
  })
  public void andOrNot(String testcase) {
    select(testcase);
  }

  /** https://issues.folio.org/browse/DMOD-184 CQL conversion seems to ignore some errors */
  @Test
  public void startsWithOr() {
    cql2pgJsonException("or name=a", QueryValidationException.class);
  }

  @Test
  public void prox() {
    cql2pgJsonException("name=Lea prox/unit=word/distance>3 name=Long",
        CQLFeatureUnsupportedException.class, "CQLProxNode");
  }

  @Test
  @Parameters({
    "long                           # Lea Long",
    "LONG                           # Lea Long",
    "lONG                           # Lea Long",
    "\"lEA LoNg\"                   # Lea Long",
    //"name == \"LEA long\"         # Lea Long",
    "name == \"Lea Long\"           # Lea Long",
  })
  public void caseInsensitive(String testcase) {
    select(testcase);
  }
  @Test
  @Parameters({
    "name ==\"LEA long\"           #", // == means field match
  })
  public void caseSensitive(String testcase) {
    select(cql2pgjsonRespectCase,testcase);
  }

  @Test
  /* fulltext only supports right truncation
    "*Lea* *Long*                   # Lea Long",
    "*e* *on*                       # Lea Long",
    "?e? ?on?                       # Lea Long",
    "L*e*a L*o*n*g                  # Lea Long",
    "Lo??                           # Lea Long",
    "Lo?                            #",
    "Lo???                          #",
    "??a                            # Lea Long",
    "???a                           #",
    "?a                             # Ka Keller", // and not Lea
    "name=/masked ?a                # Ka Keller",
     */
  @Parameters({
    "name=Lea                       # Lea Long",
    "name=Long                      # Lea Long",
    "name=Lo*                       # Lea Long",
    "lon*                           # Lea Long", // email matches the old way
    "*                       # Jo Jane; Ka Keller; Lea Long"
  })
  public void wildcards(String testcase) {
    select(testcase);
  }

  @Test
  public void masked() throws CQLFeatureUnsupportedException {
    select("name=/masked Lea  # Lea Long");

    ModifierSet modifierSet = new ModifierSet("base");
    modifierSet.addModifier("masked");
    CqlModifiers cqlModifiers = new CqlModifiers(modifierSet);
    assertThat(cqlModifiers.getCqlMasking(), is(CqlMasking.MASKED));
  }

  @Test
  public void masking() {
    cql2pgJsonException("email=/unmasked Lea", CQLFeatureUnsupportedException.class, "unmasked");
    cql2pgJsonException("email=/substring Lea", CQLFeatureUnsupportedException.class, "substring");
    cql2pgJsonException("email=/regexp Lea", CQLFeatureUnsupportedException.class, "regexp");
  }

  @Test
  @Parameters({
    "email==\\\\                    # a",  // 1 backslash, masking: x 2 for CQL, x 2 for Java
    "email==\\\\\\\\                # b",  // 2 backslashs, masking: x 2 for CQL, x 2 for Java
    "email==\\*                     # c",
    "email==\\*\\*                  # d",
    "email==\\?                     # e",
    "email==\\?\\?                  # f",
    "email==\\\\\\\"                # g",
    "email==\\\\\\\"\\\\\\\"        # h",
    "             address.zip=1     # a",
    "'         OR address.zip=1     # a",
    "name=='   OR address.zip=1     # a",
    "name==\\  OR address.zip=1     # a",
    "a                              # a",
    "h                              # h",
    "\\a                            # a",
    "\\h                            # h"
  })
  public void special(String testcase) {
    select("special.sql", testcase);
  }

  @Test
  @Parameters({
    // fulltext (used by =) removes any quote and backslash
    "email =\"\\\"\"     #  ",   // email ="\""
    "email==\"\\\"\"     # a",   // email=="\""
    "email =\"a\\\"b\"   # b",   // email ="a\"b"
    "email==\"a\\\"b\"   # b",   // email=="a\"b"
    "email =\"\\\"\\\\\" #  ",   // email ="\"\\"
    "email==\"\\\"\\\\\" # c",   // email=="\"\\"
  })
  public void quotes(String testcase) {
    select("quotes.sql", testcase);
  }

  @Test
  @Parameters({
    // The = operator is word based. An empty string or string with whitespace only contains no word at all so
    // there is no matching restriction - resulting in matching anything (that is defined and not null).
    "email=\"\"                         # e2; e3; e4",
    "email=\" \t \t \"                  # e2; e3; e4",
    "email==\"\"                        # e2",      // the == operator matches the complete string
    "email<>\"\"                        # e3; e4",  // the <> operator matches the complete string
    "address.city =  \"\"               # c2; c3; c4",
    "address.city == \"\"               # c2",
    "address.city <> \"\"               # c3; c4",  // same as example from CQL spec: dc.identifier <> ""
    "email=e                            # e4",
    "cql.allRecords=1 NOT email=e       # c0; c1; c2; c3; c4; e1; e2; e3; n",
    "email=\"\"       NOT email=e       # e2; e3",
    "cql.allRecords=1 NOT email=\"\"    # c0; c1; c2; c3; c4; e1; n",
    "cql.allRecords=1 NOT email==\"\"   # c0; c1; c2; c3; c4; e1; e3; e4; n",
    "email=\"\"       NOT email==\"\"   # e3; e4",
  })
  public void fieldExistsOrEmpty(String testcase) {
    select("existsEmpty.sql", testcase);
  }

  @Test
  @Parameters({
    "                     lang == []                      # a",
    "cql.allRecords=1 NOT lang <> []                      # a; n",
    "lang = en                                # b; c; d; f; g; h; i",

    // note that \"en\" also matches case f ["\"en"]
    "                     lang = \\\"en\\\"   # b; c; d; f; g; h; i",  // without Java quoting: \"en\"
    "cql.allRecords=1 NOT lang = \\\"en\\\"   # a; e; n",
    "lang = \"\"      NOT lang = \\\"en\\\"   # a; e",
    "lang = \"\"                                                         # a; b; c; d; e; f; g; h; i",
    "cql.allRecords=1 NOT lang = \"\"                                    # n",
  })
  public void array(String testcase) {
    select("array.sql", testcase);
  }

  @Test
  // Should not produce a StackOverflowError:
  // https://issues.folio.org/browse/CIRC-119 "Requests API GET /requests does not scale"
  public void matchAnyFromLongList() {
    select("name==(a or b or c or d or e or f or g or h or j or k or l or m or n or o or p or q or s or t or u or v "
      + "or w or x or y or z or 0 or 1 or 2 or 3 or 4 or 5 or 6 or 7 or 8 or 9 or \"Jo Jane\")  # Jo Jane");
  }

  //@Test
  // The fulltext does not support caret anchoring. We do not have enough
  // data in the email field to make a maeningful test. Anyway, the serverchoice
  // tries to match the name first, and that throws a QueryValidationException
  // on the caret, so we never try the old-fashioned way.
  @Parameters({
    "^Jo                            # Jo Jane",
    "Jo^                            #",
    "Jo^ Jane                       #",
    "^Jane                          #",
    "Jo ^Jane                       #",
    "Jane^                          # Jo Jane",
    "^Jo Jane^                      # Jo Jane",
    "name any \"Jane^ ^Jo\"         # Jo Jane",
  })
  public void caret(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "address.city= Søvang            # Lea Long",
    "address.city==Søvang            # Lea Long",
    "address.city= øvang             #",
    "address.city==øvang             #",
    "address.city= vang              #",
    "address.city= SØvang            # Lea Long",
    "address.city==SØvang            # Lea Long",
    "address.city= Sovang            # Lea Long",
    "address.city==Sovang            # Lea Long",
    "address.city= Sövang            # Lea Long",
    "address.city==Sövang            # Lea Long",
    "address.city= SÖvang            # Lea Long",
    "address.city==SÖvang            # Lea Long",
    "address.city= Sävang            #",
    "address.city==Sävang            #",
    "address.city= SÄvang            #",
    "address.city==SÄvang            #",
  })
  public void unicode(String testcase) {
    select(testcase);
    select(testcase.replace("==", "==/ignoreCase/ignoreAccents ")
                   .replace("= ", "= /ignoreCase/ignoreAccents "));
  }

  @Test
  @Parameters({
    "address.city== Søvang # Lea Long",
    "address.city== Sovang # Lea Long",
    "address.city== Sövang # Lea Long",
    "address.city== SØvang #",
    "address.city== SOvang #",
    "address.city== SÖvang #",
    // the same using = is a full text search and
    // full text does not support respect case.
  })
  public void unicodeCase(String testcase) {
    select(cql2pgjsonRespectCase, testcase);
  }

  @Ignore("Needs locale/collation. Currently is C.")
  @Test
  @Parameters({
    "address.city=  SØvang # Lea Long",
    "address.city== SØvang # Lea Long",
 })
  public void unicodeAccentsNonWindows(String testcase) {
    select(cql2pgjsonRespectAccents, testcase);
    select(cql2pgjsonRespectCase, testcase);
  }

  @Test
  @Parameters({
    "address.city=  Søvang # Lea Long",
    "address.city== Søvang # Lea Long",
    "address.city=  Sovang #",
    "address.city== Sovang #",
    "address.city=  SOvang #",
    "address.city== SOvang #",
    "address.city=  Sövang #",
    "address.city== Sövang #",
    "address.city=  SÖvang #",
    "address.city== SÖvang #",
  })
  public void unicodeAccents(String testcase) {
    select(cql2pgjsonRespectAccents, testcase);
  }

  @Test
  @Parameters({
    "address.city== Søvang # Lea Long",
    "address.city== Sovang #",
    "address.city== SOvang #",
    "address.city== Sövang #",
    "address.city== SÖvang #",
    // the same using = is a full text search and
    // full text does not support respect case.
  })
  public void unicodeCaseAccents(String testcase) {
    select(cql2pgjsonRespectBoth, testcase);
  }

  @Test
  @Parameters({
    "address.city== S*      # Jo Jane; Lea Long",
    "address.city<> S*      # Ka Keller",
    "address.city== S*g     # Lea Long",
    "address.city<> S*g     # Jo Jane; Ka Keller",
    "address.city== Sø*     # Lea Long",
    "address.city== Sø*g    # Lea Long",
    "address.city== ?øvang  # Lea Long",
    "address.city== S?vang  # Lea Long",
    "address.city== Søvan?  # Lea Long",
    "address.city== *Søvang # Lea Long",
    "address.city== **v**   # Jo Jane; Lea Long",
    "address.city== **?y?** # Jo Jane",
    "address.city== søvang  #",         // lowercase
  })
  public void like(String testcase) throws QueryValidationException {
    select(cql2pgjsonRespectBoth, testcase);
    String sql = cql2pgjsonRespectBoth.toSql(testcase.substring(0, testcase.indexOf('#'))).getWhere();
    assertThat(sql, containsString(" LIKE "));
  }

  @Test
  @Parameters({
    "*         sortBy name                           # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy id                             # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy id/sort.ascending              # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy id/sort.descending             # Lea Long; Ka Keller; Jo Jane",
    "*         sortBy groupId                        # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy groupId/sort.ascending         # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy groupId/sort.descending        # Lea Long; Ka Keller; Jo Jane",
    "*         sortBy name/sort.ascending            # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy name/sort.ascending/string     # Jo Jane; Ka Keller; Lea Long",
    "*         sortBy name/sort.descending           # Lea Long; Ka Keller; Jo Jane",
    "*         sortBy name/sort.descending/string    # Lea Long; Ka Keller; Jo Jane",
    "*         sortBy alternateEmail                 # Jo Jane; Lea Long; Ka Keller",
    "*         sortBy alternateEmail/sort.ascending  # Jo Jane; Lea Long; Ka Keller",
    "*         sortBy alternateEmail/sort.descending # Ka Keller; Lea Long; Jo Jane",
    "*         sortBy address.zip                    # Ka Keller; Jo Jane; Lea Long",
    "name=\"\" sortBy name                           # Jo Jane; Ka Keller; Lea Long",
    "name=\"\" sortBy name/sort.ascending            # Jo Jane; Ka Keller; Lea Long",
    "name=\"\" sortBy name/sort.ascending            # Jo Jane; Ka Keller; Lea Long",
    "name=\"\" sortBy name/sort.descending           # Lea Long; Ka Keller; Jo Jane",
    "name=\"\" sortBy name/sort.descending           # Lea Long; Ka Keller; Jo Jane",
    "name=\"\" sortBy address.zip                    # Ka Keller; Jo Jane; Lea Long",
  })
  public void sort(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "cql.allRecords=1 sortBy address.zip/sort.ascending/number  name # a; b; c; d; e; f; g; h",
    "cql.allRecords=1 sortBy address.zip/sort.descending/number name # h; g; d; e; f; c; b; a",
  })
  public void sortNumber(String testcase) {
    select("special.sql", testcase);
  }

  @Test
  @Parameters({
    "address.zip</number 1                  #",
    "address.zip</number 2                  # a",
    "address.zip</number 3                  # a; b",
    "address.zip<=/number 0                 #",
    "address.zip<=/number 1                 # a",
    "address.zip<=/number 2                 # a; b",
    "address.zip>/number 16                 # g; h",
    "address.zip>/number 17                 # h",
    "address.zip>/number 18                 #",
    "address.zip>=/number 17                # g; h",
    "address.zip>=/number 18                # h",
    "address.zip>=/number 19                #",
    "address.zip=/number 4                  # d; e; f",
    "address.zip==/number 4                 # d; e; f",
    "address.zip=/number 4.0                # d; e; f",
    "address.zip==/number 4.0               # d; e; f",
    "address.zip=/number 4e0                # d; e; f",
    "address.zip==/number 4e0               # d; e; f",
    "address.zip<>/number 4                 # a; b; c; g; h",
    "address.zip<>/number 4.0               # a; b; c; g; h",
    "address.zip<>/number 4e0               # a; b; c; g; h",
  })
  public void compareNumber(String testcase) throws CQL2PgJSONException {
    select("special.sql", testcase);
  }

  @Test
  @Parameters({
    "address.city =    1234                 # e; f",
    "address.city all  1234                 # e; f",
    "address.city ==   1234                 # e",
    "address.city =   01234                 # g; h",
    "address.city all 01234                 # g; h",
    "address.city ==  01234                 # g",
  })
  public void numberInStringField(String testcase) throws CQL2PgJSONException {
    select("special.sql", testcase);
  }

  @Test
  @Parameters({
    "name< \"Ka Keller\"  # Jo Jane",
    "name<=\"Ka Keller\"  # Jo Jane; Ka Keller",
    "name> \"Ka Keller\"  # Lea Long",
    "name>=\"Ka Keller\"  # Ka Keller; Lea Long",
    "name<>\"Ka Keller\"  # Jo Jane; Lea Long",
    "name<>4              # Jo Jane; Ka Keller; Lea Long",
    "name=4               #",
    // "fff" + 600 x "f" + "gfffffffffff@example.com"  # Jo Jane
    // "fff" + 600 x "f" + "hfffffffffff@example.com"  # Lea Long
    // "ffg" + 600 x "f" + "fffffffffffg@example.com"  # Ka Keller
    // compare against Jo Jane's value:
    "alternateEmail< \"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" #",
    "alternateEmail<=\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" # Jo Jane",
    "alternateEmail> \"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" #  Ka Keller; Lea Long",
    "alternateEmail>=\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffgfffffffffff@example.com\" # Jo Jane; Ka Keller; Lea Long",
    // compare against Lea Long's value:
    "alternateEmail< \"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Jo Jane",
    "alternateEmail<=\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Jo Jane; Lea Long",
    "alternateEmail> \"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Ka Keller",
    "alternateEmail>=\"fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffhfffffffffff@example.com\" # Ka Keller; Lea Long",
  })
  public void compareString(String testcase) {
    select(testcase);
  }

  @Test
  @Parameters({
    "cql.allRecords=1                             # Jo Jane; Ka Keller; Lea Long",
    "cql.allRecords=1 NOT name=Jo                 # Ka Keller; Lea Long",
    "cql.allRecords=0                             # Jo Jane; Ka Keller; Lea Long",
    "cql.allRecords=0 OR name=Jo                  # Jo Jane; Ka Keller; Lea Long",
    "cql.allRecords=1 sortBy name/sort.descending # Lea Long; Ka Keller; Jo Jane",
  })
  public void allRecords(String testcase) {
    select(testcase);
  }

  @Test(expected = FieldException.class)
  public void nullField() throws FieldException {
    String s = null;
    new CQL2PgJSON(s);
  }

  @Test(expected = FieldException.class)
  public void emptyField() throws FieldException {
    new CQL2PgJSON(" \t \t ");
  }

  @Test
  public void noServerChoiceIndexes() throws IOException, CQL2PgJSONException {
    cql2pgJsonException(new CQL2PgJSON("users.user_data", Arrays.asList()),
        "Jane", QueryValidationException.class, "serverChoiceIndex");
    cql2pgJsonException(new CQL2PgJSON("users.user_data", (List<String>) null),
        "Jane", QueryValidationException.class, "serverChoiceIndex");
  }

  @Test
  public void prefixNotImplemented() throws FieldException, RuntimeException {
    cql2pgJsonException(new CQL2PgJSON("users.user_data"),
        "> n = name n=Ka", CQLFeatureUnsupportedException.class, "CQLPrefixNode");
  }

  @Test
  public void relationNotImplemented() throws FieldException, RuntimeException {
    cql2pgJsonException(new CQL2PgJSON("users.user_data"),
        "address.zip encloses 12", CQLFeatureUnsupportedException.class, "Relation", "encloses");
  }

  @Test(expected = ServerChoiceIndexesException.class)
  public void nullIndex() throws CQL2PgJSONException {
    new CQL2PgJSON("users.user_data", Arrays.asList((String) null));
  }

  @Test(expected = ServerChoiceIndexesException.class)
  public void emptyIndex() throws CQL2PgJSONException {
    new CQL2PgJSON("users.user_data", Arrays.asList(" \t \t "));
  }

  @Test(expected = ServerChoiceIndexesException.class)
  public void doubleQuoteIndex() throws CQL2PgJSONException {
    new CQL2PgJSON("users.user_data", Arrays.asList("test\"cql"));
  }

  @Test(expected = ServerChoiceIndexesException.class)
  public void singleQuoteIndex() throws CQL2PgJSONException {
    new CQL2PgJSON("users.user_data", Arrays.asList("test'cql"));
  }

  @Test(expected = FieldException.class)
  public void nullFieldList() throws CQL2PgJSONException {
    new CQL2PgJSON((List<String>) null);
  }

  @Test(expected = FieldException.class)
  public void emptyFieldList() throws CQL2PgJSONException {
    new CQL2PgJSON(Arrays.asList());
  }

  @Test
  public void singleField() throws CQL2PgJSONException {
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON(Arrays.asList("users.user_data"), Arrays.asList("name"));
    select(aCql2pgJson, "Long   # Lea Long");
  }

  @Test
  public void toSqlSimple() throws QueryValidationException {
    SqlSelect s = cql2pgJson.toSql("cql.allRecords=1");
    assertEquals("true", s.getWhere());
    assertEquals("", s.getOrderBy());
    assertEquals("WHERE true", s.toString());
  }

  @Test
  public void toSql() throws QueryValidationException {
    SqlSelect s = cql2pgJson.toSql("email=Long sortBy name/sort.descending");
    assertThat(s.getWhere(),
        allOf(containsString("get_tsvector"),
            containsString("users.user_data->>'email'")));
    assertEquals("left(lower(f_unaccent(users.user_data->>'name')),600) DESC, lower(f_unaccent(users.user_data->>'name')) DESC", s.getOrderBy());
    String sql = s.toString();
    assertTrue(sql.startsWith("WHERE get_tsvector("));
    assertTrue(sql.endsWith(" ORDER BY "
      + "left(lower(f_unaccent(users.user_data->>'name')),600) DESC, lower(f_unaccent(users.user_data->>'name')) DESC"));
  }

  @Test
  public void optimizedOR() throws QueryValidationException {
    SqlSelect s = cql2pgJson.toSql("name=* OR email=*");
    assertEquals("true", s.getWhere());
    s = cql2pgJson.toSql("name=* OR email=* OR zip=*");
    assertEquals("true", s.getWhere());
    s = cql2pgJson.toSql("name=\"\"");  // any that has a name
    assertEquals("users.user_data->>'name' ~ ''", s.getWhere());
    s = cql2pgJson.toSql("name=\"\" OR email=\"\"");
    assertEquals("(users.user_data->>'name' ~ '') OR (users.user_data->>'email' ~ '')", s.getWhere());
  }

  @Test(expected = QueryValidationException.class)
  public void toSqlException() throws QueryValidationException {
    cql2pgJson.toSql("");
  }

  @Test
  @Parameters({
    // id=* matches all records, special case mentioned in RMB docs
    // name="" matches all records where name is defined, special case mentioned in RMB docs
    // name=="" matches all records where name is defined and empty, mentioned in RMB docs
    "id=*,     true",
    "id=\"\",  true",  // id is always defined, primary key
    "id==*,    true",  // id is always defined, primary key
    "id==\"\", false", // id is always defined and always not the empty string
    "id<>*,    false",
    "id<>\"\", true",
    "groupId=*,                                  groupId IS NOT NULL",
    "groupId=\"\",                               groupId IS NOT NULL",
    "groupId==*,                                 groupId IS NOT NULL",
    "groupId==\"\",                              false",
    "groupId<>*,                                 false",
    "groupId<>\"\",                              groupId IS NOT NULL",
    "id=\"11111111-1111-1111-1111-111111111111\",              id='11111111-1111-1111-1111-111111111111'",
    "id=\"2*\",                                       (id BETWEEN '20000000-0000-0000-0000-000000000000' "
                                                           + "AND '2fffffff-ffff-ffff-ffff-ffffffffffff')",
    "id=\"22222222*\",                                (id BETWEEN '22222222-0000-0000-0000-000000000000' "
                                                           + "AND '22222222-ffff-ffff-ffff-ffffffffffff')",
    "groupId=\"22222222*\",                      (groupId BETWEEN '22222222-0000-0000-0000-000000000000' "
                                                           + "AND '22222222-ffff-ffff-ffff-ffffffffffff')",
    "groupId==\"22222222*\",                     (groupId BETWEEN '22222222-0000-0000-0000-000000000000' "
                                                           + "AND '22222222-ffff-ffff-ffff-ffffffffffff')",
  })
  public void idColumnRelation(String cql, String expectedSql) throws QueryValidationException {
    assertEquals(expectedSql, cql2pgJson.toSql(cql).getWhere());
  }

  @Test
  @Parameters({
    "cql.allRecords=1 sortBy id                               , WHERE true ORDER BY id     ",
    "cql.allRecords=1 sortBy id/number                        , WHERE true ORDER BY id     ",
    "cql.allRecords=1 sortBy id/sort.descending               , WHERE true ORDER BY id DESC",
    "cql.allRecords=1 sortBy id/sort.descending age/number id , WHERE true ORDER BY id DESC\\, users.user_data->'age'\\, id",
    "cql.allRecords=1 sortBy groupId                          , WHERE true ORDER BY groupId",
  })
  public void idColumnSort(String cql, String expectedSql) throws CQL2PgJSONException {
    CQL2PgJSON c = new CQL2PgJSON("users.user_data");
    assertEquals(expectedSql, c.toSql(cql).toString());
  }

  @Test
  @Parameters({
    "id=11111111-1111-1111-1111-111111111111   # Jo Jane",
    "id=22222222-2222-2222-2222-222222222222   # Ka Keller",
    "id=33333333-3333-3333-3333-33333333333a   # Lea Long",
    "id=33333333-3333-3333-3333-33333333333A   # Lea Long",
    "id==33333333-3333-3333-3333-33333333333A  # Lea Long",
    "id<>11111111-1111-1111-1111-111111111111  # Ka Keller; Lea Long",
    "id<>22222222-2222-2222-2222-222222222222  # Jo Jane; Lea Long",
    "id<>33333333-3333-3333-3333-33333333333A  # Jo Jane; Ka Keller",
    "id=zz                                     #",         // invalid UUID doesn't match any record
    "id==zz                                    #",
    "id<>zz                                    # Jo Jane; Ka Keller; Lea Long",
    "id=11111111111111111111111111111111       #",
    "id=11111111+1111-1111-1111-111111111111   #",
    "id=11111111-1111-1111-1111-11111111111    #",
    "id=11111111-1111-1111-1111-1111111111111  #",
    "id=11111111-1111-1111-1111-111111111111-1 #",
    "id=\"\"                                   # Jo Jane; Ka Keller; Lea Long", // ="" matches if id is defined (per docs)
    "id==\"\"                                  #",
    "id<>\"\"                                  # Jo Jane; Ka Keller; Lea Long",
    "id=1*                                     # Jo Jane",
    "id=1**                                    # Jo Jane",
    "id=1***                                   # Jo Jane",
    "id=1z*                                    #",
    "id<>2ä*                                   # Jo Jane; Ka Keller; Lea Long",  // a umlaut
    "id<>1*                                    # Ka Keller; Lea Long",
    "id<>2*                                    # Jo Jane; Lea Long",
    "id<>3*                                    # Jo Jane; Ka Keller",
    "id=11111111-1111-1111-1111-111111111111*  # Jo Jane", // ok to trunc after full match, the UI does
    "id=*                                      # Jo Jane; Ka Keller; Lea Long",
    "id<>*                                     #",
    "id=*1                                     # ! only right truncation supported for id",
    "id=*1*                                    # ! only right truncation supported for id",
    "id=11111111*1111-1111-1111-111111111111   # ! only right truncation supported for id",
    "id< 11111111-1111-1111-1111-111111111110  #",
    "id< 11111111-1111-1111-1111-111111111111  #",
    "id< 11111111-1111-1111-1111-111111111112  # Jo Jane",
    "id<=11111111-1111-1111-1111-111111111110  #",
    "id<=11111111-1111-1111-1111-111111111111  # Jo Jane",
    "id<=11111111-1111-1111-1111-111111111112  # Jo Jane",
    "id> 11111111-1111-1111-1111-111111111110  # Jo Jane; Ka Keller; Lea Long",
    "id> 11111111-1111-1111-1111-111111111111  # Ka Keller; Lea Long",
    "id> 11111111-1111-1111-1111-111111111112  # Ka Keller; Lea Long",
    "id>=11111111-1111-1111-1111-111111111110  # Jo Jane; Ka Keller; Lea Long",
    "id>=11111111-1111-1111-1111-111111111111  # Jo Jane; Ka Keller; Lea Long",
    "id>=11111111-1111-1111-1111-111111111112  # Ka Keller; Lea Long",
    "id>=11111111-1111-111w-1111-111111111112  # ! Invalid UUID after 'id>='",
    "id=/masked         11111111-1111-1111-1111-111111111111   # ! Unsupported modifier masked",
    "id=/regexp         11111111-1111-1111-1111-111111111111   # ! Unsupported modifier regexp",
    "groupId=\"\"                                              # Jo Jane; Ka Keller; Lea Long", // ="" matches all per docs
    "groupId==\"\"                                             #",
    "groupId=           77777777-7777-7777-7777-777777777777   # Jo Jane",
    "groupId<>          77777777-7777-7777-7777-777777777777   # Ka Keller; Lea Long",
  })
  public void idMatch(String testcase) {
    select(cql2pgJson, testcase);
  }

  //
  // Fulltext search tests
  //
  @Test
  @Parameters({
    "name=Long                      # Lea Long",
    "address.zip=2791               # Lea Long",
    "\"Lea Long\"                   # Lea Long",
    "\"Long Lea\"                   #",
    "Long                           # Lea Long",
    "Lon                            #",
    "ong                            #",
    "cql.serverChoice==example      #", // == means field match
    "email==ka@example.com          # Ka Keller",
    "email=ka@example.com           # Ka Keller",
    "email=ka@*                     # Ka Keller",
    // PG tries to be clever in tokenizing emails. Anything that looks
    // like (.+)@(.*)(\.)(??.+) is considered an email, and comes up
    // as one token. Anything else will be tokenized at the '@' and '.'
    // into separate tokens. The end result is that email-looking things
    // will only do field match, and truncation breaks (unless the top-domain
    // is longer than two characters, and we mention two characters and
    // truncate the rest)
    //
    // These tests work the same way as before:
    "jo@example.com                 # Jo Jane", // complete email works
    "email=\"com example\"          #",
    "email==example.com             #",
    // The commented-out tests below are from mostly basic(), as things
    // used to work, the uncommented tests are how PG sees things.
    "email<>example.com             # Jo Jane; Ka Keller; Lea Long",
    "email==ka@example.com          # Ka Keller",
    "name == \"Lea Long\"           # Lea Long",
    "name <> \"Lea Long\"           # Jo Jane; Ka Keller",
    "name = \"Lea *\"               # Lea Long", // Loose '*' should be ignored
    "name = \"*\"                   # Jo Jane; Ka Keller; Lea Long", // special case
    "name = \"Le* Lo*\"             # Lea Long",
    "name = \" * * \"               # Jo Jane; Ka Keller; Lea Long",
    "status = \"Active - Ready\"    # Jo Jane",
    "status = Inactive              # Ka Keller",
    "status = \"Active - Not yet\"  # Lea Long"
})
  public void basicFT(String testcase)
    throws IOException, FieldException, ServerChoiceIndexesException {
    logger.debug("basicFT: " + testcase);
    rejectLower = ! testcase.contains("==") && ! testcase.contains("<>");  // == and <> use LIKE with lower()
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON("users.user_data", Arrays.asList("name", "email", "status"));
    select(aCql2pgJson, testcase);
    logger.debug("basicFT: " + testcase + " OK ");
  }

  @Test
  @Parameters({
    // email works different, no need to test here. See basicFT()
    "name all \"\"                         # Jo Jane; Ka Keller; Lea Long",
    "name all Lea                          # Lea Long",
    "name all Long                         # Lea Long",
    "name all \"Lea Long\"                 # Lea Long",
    "name all \"Long Lea\"                 # Lea Long",
    "name all \"FooBar\"                   #",
    "name any \"\"                         # Jo Jane; Ka Keller; Lea Long",
    "name any Lea                          # Lea Long",
    "name any Long                         # Lea Long",
    "name any \"Lea Long\"                 # Lea Long",
    "name any \"Long Lea\"                 # Lea Long",
    "name any \"Lea FooBar\"               # Lea Long",
    "name any \"FooBar Long\"              # Lea Long",
    "name adj \"\"                         # Jo Jane; Ka Keller; Lea Long",
    "name adj Lea                          # Lea Long",
    "name adj Long                         # Lea Long",
    "name adj \"Lea Long\"                 # Lea Long",
    "name adj \"Long Lea\"                 #",})
  public void allAnyAdjFT(String testcase)
    throws IOException, FieldException, ServerChoiceIndexesException {
    logger.debug("allFT: " + testcase);
    rejectLower = true;
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON("users.user_data", Arrays.asList("name", "email"));
    select(aCql2pgJson, testcase);
    logger.debug("allFT: " + testcase + " OK ");
  }

  @Test
  @Parameters({
    "lang == []                    # a",
    "lang == [\"en\"]              # b",
    "lang == \"en\"                #",
    "lang = en                     # b; c; d; f; g; h; i",
    "lang = au dk                  #",
    "lang all au dk                # i",
    "lang = de dk                  # i",
    "lang = \"en-uk\"              # d",
    "lang = en-uk                  # d",
    "lang = uk-en                  #",
    "lang = uk                     # d; e",
    "contributors = terry          # a",
    "contributors = \"2b94c631-fca9-4892-a730-03ee529ffe2a\" # a",
    "contributors all \"contributornametypeid\" # a; b",
    "contributors all \"contributorNametypeid 2b94c631-fca9-4892-a730-03ee529ffe2a\" # a",
    "contributors all \"contributorNametypeid 2b94c631\" # a",
    "contributors all \"contributorNametypeid 4892\" # a",
    "contributors = \"contributorNameTypeId 2b94c631\" #",
    "identifiers = \"value 0552142352\" # a",
    "contributors = name           # a; b",
    "name = terry                  #",
    "contributors.name = terry     #",
    "contributors.name = terry     #",
    "contactInformation.phone = 0912212 # b ",
    "contactInformation.phone == 0912212 #"
    })
  public void arrayFT(String testcase) throws IOException, CQL2PgJSONException {
    logger.debug("arrayFT():" + testcase);
    rejectLower = ! testcase.contains("==");  // == uses LIKE with lower()
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON(
      "users.user_data", Arrays.asList("name"));
    select(aCql2pgJson, "array.sql", testcase);
    logger.debug("arrayFT(): " + testcase + " OK");
  }

  @Test
  @Parameters({
    "contributors =/@contributornametypeid=2b94c631-fca9-4892-a730-03ee529ffe2a terry # a",
    "contributors =/@contributornametypeid=2b94c631-fca9-4892-a730-03ee529ffe2A terry # a",
    "contributors =/@contributornametypeid=2b94c631-03ee529ffe2a terry # ",
    "contributors =/@contributornametypeid=\"2b94c631-fca9-4892-a730-03ee529ffe2a\" creator #",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator a #",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator b # b",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator c #",
    "contributors all/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator c #",
    "contributors any/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator c # b",
    "contributors =/@foo=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator c # ! CQL: Unsupported relation modifier @foo ",
    "contributors =/@foo=1/@bar=2 c # ! CQL: Unsupported relation modifier @foo ",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@bar=1 creator c # ! CQL: Unsupported relation modifier @bar",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@lang=english creator b # b",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@lang=danish creator b #",
    "contributors =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@lang=English creator b # b",
    "contributors any/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@lang English danish # b",
    "contributors all/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a/@lang English danish # ",
    "contributors =/@name/@lang terry # a",
    "contributors =/@lang=english/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a creator b # b",
    "contributors =/@lang<1 c # ! CQL: Unsupported comparison for relation modifier @lang ",
    "identifiers =/@contributornametypeid=e8b311a6-3b21-43f2-a269-dd9310cb2d0a c # ! CQL: Unsupported relation modifier @contributornametypeid",
    "identifiers =/@contributornametypeid c # ! CQL: Unsupported relation modifier @contributornametypeid",
    "name ==/@foo=bar a # ! CQL: Unsupported relation modifier @foo",
    "name = /@foo=bar a # ! CQL: Unsupported relation modifier @foo", // name has arraySubfield in schema, but no arrayModifiers
    "other= /@noArraySubfield=bar a # ! CQL: No arraySubfield defined for index",
    "contactInformation.phone == /@type=mobile 0912212 # b ",
    "contactInformation.phone == /@type=home 0912212 # ",
    "contactInformation.phone == /@type=home 091221? # b",
    "contactInformation.phone == /@type=home 09122* # b"
    })
  public void arrayRelationModifiers(String testcase) throws IOException, CQL2PgJSONException {
    logger.debug("arrayRelationModifiers():" + testcase);
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON(
      "users.user_data", Arrays.asList("name"));
    select(aCql2pgJson, "array.sql", testcase);
    logger.debug("arrayRelationModifiers(): " + testcase + " OK");
  }

  @Ignore("Need to sort out the array stuff first")
  @Test
  @Parameters({
    "lang==en   sortBy name                         # Jo Jane; Ka Keller; Lea Long",
    "example   sortBy name/sort.ascending          # Jo Jane; Ka Keller; Lea Long",
    "example   sortBy name/sort.descending         # Lea Long; Ka Keller; Jo Jane",
    "example   sortBy address.zip                  # Ka Keller; Jo Jane; Lea Long",
    "name==*a* sortBy name                         # Jo Jane; Ka Keller; Lea Long",
    "name==*a* sortBy name/sort.ascending          # Jo Jane; Ka Keller; Lea Long",
    "name==*a* sortBy name/sort.descending         # Lea Long; Ka Keller; Jo Jane",
    "name==*a* sortBy address.zip                  # Ka Keller; Jo Jane; Lea Long",
   })
  public void sortFT(String testcase) throws IOException, CQL2PgJSONException {
    logger.debug("sortFT():" + testcase);
    rejectLower = true;
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON(
      "users.user_data", Arrays.asList("name"));
    select(aCql2pgJson, testcase);
    logger.debug("sortFT(): " + testcase + " OK");
    select(testcase);
  }

  @Test(expected = QueryValidationException.class)
  public void queryByFtComparatorException() throws QueryValidationException {
    Table setup = new Table();
    setup.setTableName("test");
    cql2pgJson.queryByFt("indexText", "name=abc", "unknownComparator", null, setup);
  }

  /* And/Or/Not tests need to be done with different data, with pgs
  simple truncations. TODO.
   */

  /* Simple subqueries, on the users and groups. Users point to groups.
   *
   */
  @Ignore("Subqueries not enabled (yet)")
  @Test
  @Parameters({
    "name = Long           # Lea Long",
    "groups.name = first   # Ka Keller "
  })
  public void subFT(String testcase) throws IOException, CQL2PgJSONException {
    logger.debug("subFT():" + testcase);
    rejectLower = true;
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON("users.user_data");
    select(aCql2pgJson, testcase);
    logger.debug("subFT(): " + testcase + " OK");
  }

  /* TODO: More complex subqueries, on instances, holdings, and items.
   * Note, we query on instances, locations point to instances, items point
   * to locations.
   */
  @Ignore("TODO: The subquery stuff is not really enabled yet, except incidentally inside fulltext indexes.")
  @Test
  @Parameters({
    "name = first          # first",
    "holdings.callNumber = 10  # noloc",
    "holdings.permLoc = HP    # all; holocp; holoctp",
    "holdings.tempLoc = HT    # all; holoct; holoctp",
    "holdings.tempLoc = HT or holdings.permLoc = HP # all; holocp; holoct; holoctp",
    "name <> first            # all; holocp; holoct; holoctp; itlocp; itloct; last; noloc",
    "name <> first not name = last  # all; holocp; holoct; holoctp; itlocp; itloct; noloc",
    "holdings.tempLoc = HT not holdings.permLoc = HP # holoct",
    "cql.allRecords=1  not name = first  # all; holocp; holoct; holoctp; itlocp; itloct; last; noloc",
    "holdings.tempLoc = \"\"  # all; holoct; holoctp",
    "cql.allRecords=1  not holdings.tempLoc=\"\"  # first; holocp; itlocp; itloct; last; noloc",
    "holdings.permLoc = HP NOT holdings.tempLoc=\"\" # holocp",
    "(holdings.tempLoc = HT) or (holdings.permLoc = HP NOT holdings.tempLoc=\"\") "
    + "# all; holocp; holoct; holoctp",
    "holdings.items.permLoc=IP # all; itlocp",
    "holdings.items.tempLoc=IT # all; itloct",
    "holdings.items.permLoc = IP NOT holdings.items.tempLoc=\"\" # itlocp",
    "(holdings.items.tempLoc = IT) or (holdings.items.permLoc = IP NOT holdings.items.tempLoc=\"\") "
    + "# all; itlocp; itloct",
    // test with two holdings records, one with good permloc ZZ, one where a temp YY overrides it.
    // We should still get the instance 'all', but the NOT clause kills both holdings.
    // "(holdings.tempLoc = ZZ) or (holdings.permLoc = ZZ NOT holdings.tempLoc=\"\") # all",
    //
    // Experiments to see if I can rewrite the query
    "holdings.tempLoc = ( ZZ OR YY )# all", // expands internally to loc=ZZ or loc=YY
    "holdings.tempLoc = AA AND holdings.permLoc = ZZ # all", // expands internally to loc=ZZ or loc=YY
    //
    // Building up to the effective location test with XX
    "holdings.tempLoc = XX # holoctp", //
    "holdings.permLoc = XX NOT holdings.tempLoc=\"\" # holocp ",
    "(holdings.tempLoc = XX) or (holdings.permLoc = XX NOT holdings.tempLoc=\"\") "
    + "# holocp; holoctp",
    "holdings.items.tempLoc = XX # itloct", //
    "holdings.items.permLoc = XX NOT holdings.items.tempLoc=\"\" # itlocp ",
    "(holdings.items.tempLoc = XX) or (holdings.items.permLoc = XX NOT holdings.items.tempLoc=\"\") "
    + "# itlocp; itloct",
 // towards the final effective location clause
  /*
    "(holdings.items.tempLoc = XX) "
    + "OR (holdings.items.permLoc = XX NOT holdings.items.tempLoc=\"\") "
    + "OR (holdings.temploc = XX NOT holdings.items.tempLoc =\"\" NOT holdings.items.permLoc = \"\" )"
    + " # holoctp; itlocp; itloct "
    // This fails, the items clause masks away also the holdings temploc, althoug it is for a
    // different item.
   */ //
  })
  public void instanceSubFT(String testcase) throws IOException, CQL2PgJSONException {
    logger.debug("instanceSubFT():" + testcase);
    rejectLower = true;
    CQL2PgJSON aCql2pgJson = new CQL2PgJSON("instances.jsonb");
    select(aCql2pgJson, "instances.sql", testcase);
    logger.debug("instanceSubFT(): " + testcase + " OK");
  }

  /*
  CQL fields can be quoted and, thus, contain various characters that - if no
  properly escaped can be used to unquote the JSON path that is generated
   */

  @Test
  @Parameters({
    "\"field'))@@to_tsquery(('english\"=x # "})
  public void sqlInjectionInField(String testcase) {
    select(testcase);
  }

  @Test(expected = FieldException.class)
  public void validateFieldName() throws FieldException {
    new CQL2PgJSON("foo'bar");
  }

  @Test
  public void initDbTable() throws Throwable {
    assertThat(new CQL2PgJSON("x").initDbTable(), is(CQL2PgJSON.InitDbTableResult.NOT_FOUND));
    assertThat(new CQL2PgJSON("deleted").initDbTable(), is(CQL2PgJSON.InitDbTableResult.NOT_FOUND));
    assertThat(new CQL2PgJSON("loan").initDbTable(), is(CQL2PgJSON.InitDbTableResult.TABLE_FOUND));
    assertThat(new CQL2PgJSON("audit_loan").initDbTable(), is(CQL2PgJSON.InitDbTableResult.AUDIT_TABLE_FOUND));
    assertThat(new CQL2PgJSON(List.of("x", "y")).initDbTable(), is(CQL2PgJSON.InitDbTableResult.NO_PRIMARY_TABLE_NAME));
  }
}
