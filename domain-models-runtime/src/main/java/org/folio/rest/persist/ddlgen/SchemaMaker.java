package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.folio.dbschema.ForeignKeys;
import org.folio.dbschema.OptimisticLockingMode;
import org.folio.dbschema.Schema;
import org.folio.dbschema.Table;
import org.folio.dbschema.TableOperation;
import org.folio.dbschema.TenantOperation;
import org.folio.rest.tools.utils.RmbVersion;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

/**
 * @author shale
 *
 */
public class SchemaMaker {

  private static Configuration cfg = getConfiguration();
  private Map<String, Object> templateInput = new HashMap<>();
  private String tenant;
  private String module;
  private TenantOperation mode;
  private String previousVersion;
  private String newVersion;
  private String rmbVersion;
  private Schema schema;
  private Schema previousSchema;
  private String schemaJson = "{}";

  public SchemaMaker(String tenant, String module, TenantOperation mode, String previousVersion,
                     String newVersion) {
    this.tenant = tenant;
    this.module = module;
    this.mode = mode;
    this.previousVersion = previousVersion;
    this.newVersion = newVersion;
    this.rmbVersion = RmbVersion.getRmbVersion();
  }

  private static Configuration getConfiguration() {
    Configuration configuration = new Configuration(new Version(2, 3, 26));
    // Where do we load the templates from:
    configuration.setClassForTemplateLoading(SchemaMaker.class, "/templates/db_scripts");
    configuration.setDefaultEncoding("UTF-8");
    configuration.setLocale(Locale.US);
    configuration.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    return configuration;
  }

  public String generatePurge() throws IOException, TemplateException {
    return generateDDL("delete.ftl");
  }

  public String generateSchemas() throws IOException, TemplateException {
    return generateDDL("schemas.ftl");
  }

  public String generateCreate() throws IOException, TemplateException {
    return generateDDL("create.ftl");
  }

  private String generateDDL(String template) throws IOException, TemplateException {
    templateInput.put("myuniversity", this.tenant);

    templateInput.put("mymodule", this.module);

    templateInput.put("mode", this.mode);

    String pVersion = this.previousVersion;

    if (pVersion == null) {
      //will be null on deletes unless its read from db by rmb
      pVersion = "0.0";
    }
    if (newVersion == null) {
      newVersion = "0.0";
    }

    templateInput.put("version", pVersion);

    templateInput.put("newVersion", this.newVersion);

    //TODO - check the rmbVersion in the internal_rmb table and compare to this passed in
    //version, to check if core rmb scripts need updating due to an update
    templateInput.put("rmbVersion", this.rmbVersion);

    templateInput.put("schemaJson", this.getSchemaJson());

    templateInput.put("tables", tables());

    templateInput.put("views", this.schema.getViews());

    templateInput.put("scripts", this.schema.getScripts());

    templateInput.put("exactCount", this.schema.getExactCount()+"");

    Template tableTemplate = cfg.getTemplate(template);
    Writer writer = new StringWriter();
    tableTemplate.process(templateInput, writer);

    return writer.toString();
  }

  public String generateIndexesOnly() throws IOException, TemplateException {
    return generateDDL("indexes_only.ftl");
  }

  public static String generateOptimisticLocking(String tenant, String module, String tablename) {
    try {
      Table table = new Table();
      table.setTableName(tablename);
      table.setWithOptimisticLocking(OptimisticLockingMode.FAIL);
      Map<String, Object> parameters = new HashMap<>(3);
      parameters.put("myuniversity", tenant);
      parameters.put("mymodule", module);
      parameters.put("table", table);
      Template tableTemplate = cfg.getTemplate("optimistic_locking.ftl");
      Writer writer = new StringWriter();
      tableTemplate.process(parameters, writer);
      return writer.toString();
    } catch (IOException | TemplateException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return fieldName is the same and not null, or fieldPath is the same and not null
   */
  static boolean sameForeignKey(ForeignKeys a, ForeignKeys b) {
    return (a.getFieldName() != null && a.getFieldName().equals(b.getFieldName())) ||
        (a.getFieldPath() != null && a.getFieldPath().equals(b.getFieldPath()));
  }

  /**
   * @return the tables of schema plus tables to delete (tables that exist in previousSchema but not in schema);
   *   add foreign keys to delete (those that exist in previousSchema but not in schema).
   *   Nothing to do for indexes because rmb_internal_index handles them.
   */
  List<Table> tables() {
    if (previousSchema == null || previousSchema.getTables() == null) {
      return schema.getTables();
    }
    Map<String,Table> tableForName = new HashMap<>();
    schema.getTables().forEach(table -> tableForName.put(table.getTableName(), table));
    List<Table> list = new ArrayList<>(schema.getTables());
    previousSchema.getTables().forEach(oldTable -> {
      Table newTable = tableForName.get(oldTable.getTableName());
      if (newTable == null) {
        oldTable.setMode("delete");
        list.add(oldTable);
        return;
      }

      List<ForeignKeys> oldForeignKeys = oldTable.getForeignKeys();
      if (oldForeignKeys == null || oldForeignKeys.isEmpty()) {
        return;
      }
      List<ForeignKeys> newForeignKeys =
          newTable.getForeignKeys() == null ? Collections.emptyList() : newTable.getForeignKeys();
      List<ForeignKeys> allForeignKeys = new ArrayList<>(newForeignKeys);
      oldForeignKeys.forEach(oldForeignKey -> {
        if (newForeignKeys.stream()
            .anyMatch(newForeignKey -> sameForeignKey(oldForeignKey, newForeignKey))) {
          // an entry for oldForeignKey exists in newForeignKeys, nothing to do
          return;
        }
        oldForeignKey.settOps(TableOperation.DELETE);
        allForeignKeys.add(oldForeignKey);
      });
      newTable.setForeignKeys(allForeignKeys);
    });
    return list;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
  }

  public TenantOperation getMode() {
    return mode;
  }

  public void setMode(TenantOperation mode) {
    this.mode = mode;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
    schema.setup();
  }

  public Schema getPreviousSchema() {
    return previousSchema;
  }

  public void setPreviousSchema(Schema previousSchema) {
    this.previousSchema = previousSchema;
    if (previousSchema !=  null) {
      previousSchema.setup();
    }
  }

  public String getSchemaJson() {
    return schemaJson;
  }

  public void setSchemaJson(String schemaJson) {
    this.schemaJson = schemaJson;
  }
}
