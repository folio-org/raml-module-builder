package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.folio.rest.tools.PomReader;
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

  private static Configuration cfg;
  private Map<String, Object> templateInput = new HashMap<>();
  private String tenant;
  private String module;
  private TenantOperation mode;
  private String previousVersion;
  private String newVersion;
  private String rmbVersion;
  private Schema schema;

  /**
   * @param onTable
   */
  public SchemaMaker(String tenant, String module, TenantOperation mode, String previousVersion, String newVersion){
    if(SchemaMaker.cfg == null){
      //do this ONLY ONCE
      SchemaMaker.cfg = new Configuration(new Version(2, 3, 26));
      // Where do we load the templates from:
      cfg.setClassForTemplateLoading(SchemaMaker.class, "/templates/db_scripts");
      cfg.setDefaultEncoding("UTF-8");
      cfg.setLocale(Locale.US);
      cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    }
    this.tenant = tenant;
    this.module = module;
    this.mode = mode;
    this.previousVersion = previousVersion;
    this.newVersion = newVersion;
    this.rmbVersion = PomReader.INSTANCE.getRmbVersion();
  }

  public String generateDDL() throws IOException, TemplateException {
    return generateDDL(false);
  }

  public String generateDDL(boolean recreateIndexMode) throws IOException, TemplateException {

    templateInput.put("myuniversity", this.tenant);

    templateInput.put("mymodule", this.module);

    templateInput.put("mode", this.mode);

    if("delete".equalsIgnoreCase(this.mode.name())){
      return handleDelete();
    }

    if(this.schema == null){
      //log this
      System.out.print("Must call setSchema() first...");
      return null;
    }

    String pVersion = this.previousVersion;

    if(pVersion == null){
      //will be null on deletes unless its read from db by rmb
      pVersion = "0.0";
    }
    if(newVersion == null){
      newVersion = "0.0";
    }

    templateInput.put("version", pVersion);

    templateInput.put("newVersion", this.newVersion);

    //TODO - check the rmbVersion in the internal_rmb table and compare to this passed in
    //version, to check if core rmb scripts need updating due to an update
    templateInput.put("rmbVersion", this.rmbVersion);

    this.schema.setup();

    templateInput.put("tables", this.schema.getTables());

    templateInput.put("views", this.schema.getViews());

    templateInput.put("scripts", this.schema.getScripts());

    templateInput.put("exactCount", this.schema.getExactCount()+"");

    String template = "main.ftl";
    if(recreateIndexMode){
      template = "indexes_only.ftl";
    }
    Template tableTemplate = cfg.getTemplate(template);
    Writer writer = new StringWriter();
    tableTemplate.process(templateInput, writer);

    return writer.toString();
  }

  private String handleDelete() throws IOException, TemplateException {
    Writer writer = new StringWriter();
    Template tableTemplate = cfg.getTemplate("delete.ftl");
    tableTemplate.process(templateInput, writer);
    return writer.toString();
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
  }
}
