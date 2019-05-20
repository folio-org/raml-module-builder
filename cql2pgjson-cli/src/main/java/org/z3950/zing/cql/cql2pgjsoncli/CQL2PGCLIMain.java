package org.z3950.zing.cql.cql2pgjsoncli;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.exception.QueryValidationException;
import org.folio.cql2pgjson.model.SqlSelect;
import org.json.JSONException;
import org.json.JSONObject;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

public class CQL2PGCLIMain {

  /** allow to inject a different exit method for unit testing */
  static IntConsumer exit = System::exit;
  private static Logger logger = Logger.getLogger(CQL2PGCLIMain.class.getName());

  public static void main( String[] args ) {
    try {
      System.out.println(handleOptions(args));
    } catch( Exception e ) {
      System.err.println(String.format("Got error %s, %s: ", e.getClass().toString(),
          e.getLocalizedMessage()));
      e.printStackTrace();
      exit.accept(1);
    }
  }

  static String handleOptions(String[] args) throws
      FieldException, IOException, QueryValidationException,
      ParseException {
    Options options = new Options();

    Option database = Option.builder("t")
        .hasArg()
        .required(true)
        .desc("Postgres table name")
        .build();

    Option field = Option.builder("f")
        .hasArg()
        .required(false)
        .desc("Postgres field name")
        .build();

    Option dbschema = Option.builder("b")
        .hasArg()
        .required(false)
        .desc("Path to RMB-style schema.json to describe database")
        .build();

    options.addOption(database);
    options.addOption(field);
    options.addOption(dbschema);

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);
    CQL2PgJSON cql2pgJson = null;
    String fullFieldName = line.getOptionValue("t") + "." + line.getOptionValue("f", "jsonb");
    cql2pgJson = new CQL2PgJSON(fullFieldName);
    if(line.hasOption("b")) {
      cql2pgJson.setDbSchemaPath(line.getOptionValue("b"));
    }
    List<String> cliArgs = line.getArgList();
    String cql = cliArgs.get(0);
    return parseCQL(cql2pgJson, line.getOptionValue("t"), cql);
  }

  static String readFile(String path, Charset encoding) throws IOException
  {
    System.out.println("Reading file " + path);
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    String content = new String(encoded, encoding);
    //System.out.println(String.format("Content of %s is: %s", path, content));
    return content;
  }

  static protected String parseCQL(CQL2PgJSON cql2pgJson, String dbName, String cql) throws IOException,
    QueryValidationException {
    SqlSelect sql = cql2pgJson.toSql(cql);
    String orderby = sql.getOrderBy();
    logger.log(Level.FINE, String.format("orderby for cql query '%s' is '%s'", cql, orderby));
    if(StringUtils.isBlank(orderby)) {
      return String.format("select * from %s where %s", dbName, sql.getWhere());
    }
    return String.format("select * from %s where %s order by %s",
        dbName, sql.getWhere(), orderby);
  }

  /*
    If the string is valid JSON, read the values from the JSON object. If the
    string is a path to a JSON file, load the file and read the JSON from the
    file
  */
  static Map<String, String> parseDatabaseSchemaString(String dbsString) throws
      IOException {
    JSONObject fieldSchemaJson = null;
    Map<String, String> fieldSchemaMap = new HashMap<>();
    try {
      fieldSchemaJson = new JSONObject(dbsString);
    } catch( JSONException je ) {
      System.out.println(String.format("Unable to parse %s as JSON: %s",
          dbsString, je.getLocalizedMessage()));
    }
    if(fieldSchemaJson == null) {
      String fieldSchemaJsonText = readFile(dbsString, StandardCharsets.UTF_8);
      fieldSchemaJson = new JSONObject(fieldSchemaJsonText);
    }
    for(String key : fieldSchemaJson.keySet()) {
      String value = readFile(fieldSchemaJson.getString(key), StandardCharsets.UTF_8);
      fieldSchemaMap.put(key, value);
    }
    return fieldSchemaMap;
  }

}
