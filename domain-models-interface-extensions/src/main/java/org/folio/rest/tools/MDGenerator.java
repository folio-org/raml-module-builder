package org.folio.rest.tools;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.io.Files;

/**
 * Current generates the following:
 *
 * "id": "abc",
  "name": "abc Module",
  "provides": [
    {
      "id": "service1",
      "version": "1.0"
    },
    {
      "id": "service2",
      "version": "1.0"
    },
    {
      "id" : "service3",
      "version" : "1.0"
    }
  ],
  "routingEntries": [
    {
      "methods": [
        "GET",
        "POST",
        "PUT",
        "DELETE"
      ],
      "path": "/service1",
      "level": "30",
      "type": "request-response"
    },
    {
      "methods": [
        "GET",
        "POST",
        "PUT",
        "DELETE"
      ],
      "path": "/service2",
      "level": "30",
      "type": "request-response"
    }
  ]
 *
 */
public enum MDGenerator {

  INSTANCE;

  private static final String ROUTING_ENTRIES = "routingEntries";
  private static final String PROVIDES        = "provides";
  private static final String ID              = "id";
  private static final String NAME            = "name";

  private static final String DB_HOST = "db.host";
  private static final String DB_PORT = "db.port";
  private static final String DB_DBNAME = "db.database";
  private static final String DB_PASSWORD = "db.password";
  private static final String DB_CHARSET = "db.charset";
  private static final String DB_TO = "db.queryTimeout";

  private JsonObject md = new JsonObject();
  private JsonArray rEntries = new JsonArray();
  private JsonArray provides = new JsonArray();


  private MDGenerator() {
    md.put(ROUTING_ENTRIES, rEntries);
    md.put(PROVIDES, provides);
    String id = PomReader.INSTANCE.getModuleName();
    setID(id);
    setName(id.replaceAll("_", " ") + " Module");
    setEnvs();
  }

  private void setEnvs() {
    JsonArray envs = new JsonArray();
    envs.add(new JsonObject().put("name", DB_HOST));
    envs.add(new JsonObject().put("name", DB_PORT));
    envs.add(new JsonObject().put("name", DB_DBNAME));
    envs.add(new JsonObject().put("name", DB_PASSWORD));
    envs.add(new JsonObject().put("name", DB_CHARSET));
    envs.add(new JsonObject().put("name", DB_TO));
    md.put("env", envs);
  }

  public void addRoutingEntry(RoutingEntry re) {
    rEntries.add(re.getEntry());
  }

  public void addProvidesEntry(ProvidesEntry pe) {
    provides.add(pe.getEntry());
  }

  public void setID (String id){
    md.put(ID, id);
  }
  public void setName(String name){
    md.put(NAME, name);
  }
  public void generateMD() throws IOException{
    String genPath = System.getProperties().getProperty("project.basedir") + "/GenModuleDescriptor.json";
    Files.write(md.encodePrettily(), new File(genPath), Charset.forName("UTF-8"));
  }

  class RoutingEntry {

    private JsonObject entry  = new JsonObject();
    private JsonArray methods = new JsonArray();
    private String type       = "request-response";

    public RoutingEntry() {
      entry.put("methods", methods);
      entry.put("type", type);
    }
    public void setEntryPath(String path) {
      entry.put("path", path);
    }
    public void setLevel(String level) {
      entry.put("level", level);
    }
    public void setType(String type) {
      entry.put("type", type);
    }
    public void addMethod(String method) {
      methods.add(method.toUpperCase());
    }
    public void setMethods(JsonArray verbs) {
      methods.addAll(verbs);
    }
    public JsonObject getEntry() {
      return entry;
    }
  }

  class ProvidesEntry {

    private JsonObject entry  = new JsonObject();

    public ProvidesEntry() {
      String version = PomReader.INSTANCE.getVersion();
      entry.put("version", version.substring(0, version.indexOf(".")) + ".0");
    }
    public void setId(String id) {
      entry.put("id", id);
    }
    public JsonObject getEntry() {
      return entry;
    }
  }

  public static void main(String[] args) throws Exception {

    AnnotationGrabber.generateMappings();

  }
}
