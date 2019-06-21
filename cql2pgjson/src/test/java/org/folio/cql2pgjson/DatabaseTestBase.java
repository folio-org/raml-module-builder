package org.folio.cql2pgjson;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.folio.util.ResourceUtil;

import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

/**
 * Provide conn with a java.sql.Connection to the test database.
 * Invoke setupDatebase() (for example in a @BeforeClass function)
 * and closeDatabase() (for example in a @AfterClass function).
 */
public class DatabaseTestBase {
  private static final String DB_NAME = "test_cql2pgjson";
  private static final String EMBEDDED_USERNAME = "test";
  private static final String EMBEDDED_PASSWORD = "test";
  private static PostgresProcess postgresProcess;
  /** java.sql.Connection to be used for the tests */
  static Connection conn;

  /** pattern of the EXPAIN ANALYSE reply containing the execution time in ms as group 1 */
  private static final Pattern executionTimePattern =
      Pattern.compile("^Execution time: ([0-9.]+) ms", Pattern.MULTILINE);

  /**
   * Default constructor
   */
  public DatabaseTestBase() {
    // nothing to do
  }

  /**
   * Returns s, or "" if s is null.
   * @param s the String to test for null
   * @return s or ""
   */
  private static String nonNull(String s) {
    return StringUtils.defaultString(s);
  }

  private static String url(String host, int port, String db, String username, String password) {
    return String.format("jdbc:postgresql://%s:%s/%s?currentSchema=public&user=%s&password=%s",
        nonNull(host), port, nonNull(db), nonNull(username), nonNull(password));
  }

  /**
   * @param version  the version string to check
   * @throws UnsupportedEncodingException  if version string is less than 9.6
   */
  private static void checkVersion(String version) throws UnsupportedEncodingException {
    final String msg = "Unicode features of PostgreSQL >= 9.6 required, version is ";
    String number [] = version.split("\\.");
    int a = Integer.parseInt(number[0]);
    if (a > 9) {
      return;
    }
    if (a < 9) {
      throw new UnsupportedEncodingException(msg + version);
    }
    int b = Integer.parseInt(number[1]);
    if (b >= 6) {
      return;
    }
    throw new UnsupportedEncodingException(msg + version);
  }

  @SuppressWarnings("serial")
  static class SQLRuntimeException extends RuntimeException {
    SQLRuntimeException(String message, Exception exception) {
      super(message, exception);
    }
  }

  /**
   * Set conn to the test database.
   * <p>
   * It tries to use the first working connection of these:<br>
   * environment variables DB_HOST, DB_PORT (default 5432), DB_DATABASE, DB_USERNAME, DB_PASSWORD<br>
   * jdbc:postgresql://127.0.0.1:5432/test?currentSchema=public&user=test&password=test<br>
   * jdbc:postgresql://127.0.0.1:5433/postgres?currentSchema=public&user=postgres&password=postgres<br>
   * starting an new embedded postgres
   */
  public static void setupDatabase() {
    List<String> urls = new ArrayList<>(3);
    int port = 5432;
    try {
      port = Integer.parseInt(System.getenv("DB_PORT"));
    } catch (NumberFormatException e) {
      // ignore, still use 5432
    }
    String username = nonNull(System.getenv("DB_USERNAME"));
    if (! username.isEmpty()) {
      urls.add(url(
          System.getenv("DB_HOST"),
          port,
          System.getenv("DB_DATABASE"),
          System.getenv("DB_USERNAME"),
          System.getenv("DB_PASSWORD")
          ));
    }

    // often used local test database
    urls.add("jdbc:postgresql://127.0.0.1:5432/postgres?currentSchema=public&user=test&password=test");
    // local test database of folio.org CI environment
    urls.add("jdbc:postgresql://127.0.0.1:5433/postgres?currentSchema=public&user=postgres&password=postgres");
    for (String url : urls) {
      try {
        System.out.println(url);
        conn = DriverManager.getConnection(url + "&ApplicationName=" + CQL2PgJSONTest.class.getName());
        checkVersion(conn.getMetaData().getDatabaseProductVersion());
        if ("postgres".equals(conn.getCatalog())) {
          try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP DATABASE IF EXISTS " + DB_NAME);
            stmt.executeUpdate("CREATE DATABASE " + DB_NAME + " TEMPLATE=template0 ENCODING='UTF8' LC_COLLATE='C' LC_CTYPE='C'");
          }
          catch (SQLException e) {
            System.out.println(e.getMessage());
            // ignore because the database might already be there and some other connection is open
          }
          conn.close();
          String url2 = url.replaceFirst("/postgres\\b", "/" + DB_NAME);
          System.out.println(url2);
          conn = DriverManager.getConnection(url);
        }
        return;
      }
      catch (SQLException|UnsupportedEncodingException e) {
        System.out.println(e.getMessage());
        // ignore and try next
      }
    }

    // start embedded Postgres
    try {
      final PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
      final PostgresConfig config = PostgresConfig.defaultWithDbName(DB_NAME, EMBEDDED_USERNAME, EMBEDDED_PASSWORD);
      config.getAdditionalInitDbParams().addAll(Arrays.asList(  // no not use the operating system's locale
          "-E", "UTF-8",
          "--locale=C",
          "--lc-collate=C",
          "--lc-ctype=C"
      ));
      String url = url(
          config.net().host(),
          config.net().port(),
          config.storage().dbName(),
          config.credentials().username(),
          config.credentials().password()
          );
      PostgresExecutable exec = runtime.prepare(config);
      postgresProcess = exec.start();
      conn = DriverManager.getConnection(url);
    } catch (IOException | SQLException e) {
      throw new SQLRuntimeException(e.getMessage(), e);
    }
  }


  /**
   * Close conn and stop embedded progress if needed.
   * @throws SQLRuntimeException on database error
   */
  public static void closeDatabase() {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new SQLRuntimeException(e.getMessage(), e);
      }
      conn = null;
    }
    if (postgresProcess != null) {
      postgresProcess.stop();
      postgresProcess = null;
    }
  }

  /**
   * Run the SQL statement on conn.
   * @param sqlStatement  the SQL command to run
   * @throws RuntimeException on SQLException
   */
  static void runSqlStatement(String sqlStatement) {
    try (Statement statement = conn.createStatement()) {
      statement.execute(sqlStatement);
    } catch (SQLException e) {
      throw new SQLRuntimeException(sqlStatement, e);
    }
  }

  /**
   * Run the selectStatement and return the first column of the result.
   * @param sqlStatement  the SELECT command to run
   * @return the first column of the result, converted into Strings
   * @throws SQLRuntimeException on SQLException
   */
  static List<String> firstColumn(String selectStatement) {
    try (Statement statement = conn.createStatement();
         ResultSet resultSet = statement.executeQuery(selectStatement)) {
      List<String> array = new ArrayList<>();
      while (resultSet.next()) {
        array.add(resultSet.getString(1));
      }
      return array;
    } catch (SQLException e) {
      throw new SQLRuntimeException(selectStatement, e);
    }
  }

  /**
   * Run the SQL statement on conn with EXPLAIN ANALYSE.
   * @param sqlStatement  the SQL command to run (without prepended EXPLAIN ANALYSE).
   * @return the answer from the database
   * @throws RuntimeException on SQLException
   */
  static String explainAnalyseSql(String sqlStatement) {
    String explainAnalyseSqlStatement = "EXPLAIN ANALYSE " + sqlStatement;
    try (Statement statement = conn.createStatement();
         ResultSet resultSet = statement.executeQuery(explainAnalyseSqlStatement);
        ) {
      StringBuilder result = new StringBuilder();
      while (resultSet.next()) {
        result.append(resultSet.getString(1)).append('\n');
      }
      return result.toString();
    } catch (SQLException e) {
      throw new SQLRuntimeException(explainAnalyseSqlStatement, e);
    }
  }

  static class AnalyseResult {
    /** The analyse message from Postgres */
    private final String message;
    /** The execution time in ms */
    private final float executionTimeInMs;
    /**
     * Set message and execution time.
     * @param message  analyse message from Postgres
     * @param executionTimeMs  execution time in ms as reported by Postgres.
     */
    public AnalyseResult(String message, float executionTimeMs) {
      this.message = message;
      this.executionTimeInMs = executionTimeMs;
    }

    /**
     * @return the analyse message from Postgres.
     */
    public String getMessage() {
      return message;
    }

    /**
     * @return the execution time in ms as reported by Postgres.
     */
    public float getExecutionTimeInMs() {
      return executionTimeInMs;
    }
  }

  /**
   * Run sql with "EXPLAIN ANALYSE " prepended.
   * @param sql  SQL command to run and analyse
   * @return analyse result of the sql query
   */
  static AnalyseResult analyse(String sql) {
    String msg = explainAnalyseSql(sql);
    Matcher matcher = executionTimePattern.matcher(msg);
    if (! matcher.find()) {
      throw new IllegalStateException("Execution time not found:\n" + msg);
    }
    String ms = matcher.group(1);
    return new AnalyseResult(msg, Float.parseFloat(ms));
  }

/**
   * Run SQL commands on conn. Each command can be split over several lines.
   * <p>
   * Example usage:
   * <pre>
   * {@code
   * runSql("SELECT * FROM t",
   *        "WHERE t.i=5;",
   *        "SELECT * FROM t",
   *        "WHERE t.v LIKE '%x%';");
   * }
   * </pre>
   *
   * @param lines  the strings that are the SQL statements.
   * @throws RuntimeException on SQLException
   */
  static void runSql(String ... lines) {
    String [] statements = StringUtils.join(lines, '\n').split(";\\s*[\\n\\r]+\\s*");
    for (String sql : statements) {
      runSqlStatement(sql);
    }
  }

  /**
   * Run the SQL commands in sqlFile.
   * @param path resource path to the SQL file
   * @throws RuntimeException on SQLException
   */
  static void runSqlFile(String path) {
    String file = ResourceUtil.asString(path);
    // replace \r and \n inside of $$ and $$ by space
    String [] chunks = file.split("\\$\\$");
    for (int i = 1; i < chunks.length; i += 2) {
      chunks[i] = chunks[i].replaceAll("[\\n\\r]+", " ");
    }
    // split at semicolon at end of line (removing optional whitespace)
    String [] statements = String.join("$$", chunks).split(";\\s*[\\n\\r]+\\s*");
    for (String sql : statements) {
      runSqlStatement(sql);
    }
  }
}
