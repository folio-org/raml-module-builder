package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.utils.ObjectMapperTool;

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
  private String mode;
  private List<Table> tables = new ArrayList<>();
  private List<View> views = new ArrayList<>();
  private double version;

  /**
   * @param onTable
   */
  public SchemaMaker(String tenant, String module, String mode, double version){
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
    this.version = version;
  }

  public String generateDDL() throws IOException, TemplateException {

    templateInput.put("myuniversity", this.tenant);

    templateInput.put("mymodule", this.module);

    templateInput.put("mode", this.mode);

    templateInput.put("version", this.version);

    int size = tables.size();

    for (int i = 0; i < size; i++) {
      Table t = tables.get(i);

      List<DeleteFields> dFields = t.getDeleteFields();
      if(dFields != null){
        for (int j = 0; j < dFields.size(); j++) {
          DeleteFields d = dFields.get(j);
          d.setFieldPath(convertDotPath2PostgresMutateNotation(d.getFieldName()));
        }
      }

      List<AddFields> aFields = t.getAddFields();
      if(aFields != null){
        for (int j = 0; j < aFields.size(); j++) {
          //NOTE, SINGLE QUOTES IN DEFAULT VALUE MUST BE ESCAPED - ' -> '' BY CALLER
          AddFields a = aFields.get(j);
          a.setFieldPath(convertDotPath2PostgresMutateNotation(a.getFieldName()));
        }
      }

      List<ForeignKeys> fKeys = t.getForeignKeys();
      if(fKeys != null){
        for (int j = 0; j < fKeys.size(); j++) {
          ForeignKeys f = fKeys.get(j);
          f.setFieldPath(convertDotPath2PostgresNotation(f.getFieldName()));
          f.setFieldName(f.getFieldName().replaceAll("\\.", "_"));
        }
      }

      List<TableIndexes> tInd = t.getLikeIndex();
      if(tInd != null){
        for (int j = 0; j < tInd.size(); j++) {
          TableIndexes ti = tInd.get(j);
          ti.setFieldPath(convertDotPath2PostgresNotation(ti.getFieldName()));
          ti.setFieldName(ti.getFieldName().replaceAll("\\.", "_"));
        }
      }

      List<TableIndexes> uInd = t.getUniqueIndex();
      if(uInd != null){
        for (int j = 0; j < uInd.size(); j++) {
          TableIndexes u = uInd.get(j);
          u.setFieldPath(convertDotPath2PostgresNotation(u.getFieldName()));
          //remove . from path since this is incorrect syntax in postgres
          u.setFieldName(u.getFieldName().replaceAll("\\.", "_"));
        }
      }

      List<TableIndexes> gInd = t.getGinIndex();
      if(gInd != null){
        for (int j = 0; j < gInd.size(); j++) {
          TableIndexes u = gInd.get(j);
          u.setFieldPath(convertDotPath2PostgresNotation(u.getFieldName()));
          //remove . from path since this is incorrect syntax in postgres
          u.setFieldName(u.getFieldName().replaceAll("\\.", "_"));
        }
      }
    }

    size = views.size();
    for (int i = 0; i < size; i++) {
      View v = views.get(i);
      ViewTable vt = v.getJoinTable();
      vt.setJoinOnField(convertDotPath2PostgresNotation( vt.getJoinOnField() ));
      vt = v.getTable();
      vt.setJoinOnField(convertDotPath2PostgresNotation( vt.getJoinOnField() ));
    }

    templateInput.put("tables", this.tables);

    templateInput.put("views", this.views);


    Template tableTemplate = cfg.getTemplate("main.ftl");

    Writer writer = new StringWriter();
    tableTemplate.process(templateInput, writer);

    return writer.toString();
  }

  public List<Table> getTables() {
    return tables;
  }

  public void setTables(List<Table> tables) {
    this.tables = tables;
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

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public List<View> getViews() {
    return views;
  }

  public void setViews(List<View> views) {
    this.views = views;
  }

  public static void main(String args[]) throws Exception {

    SchemaMaker fm = new SchemaMaker("harvard", "mod_users", "create", 1.0);

    String json = IOUtils.toString(
      SchemaMaker.class.getClassLoader().getResourceAsStream("templates/db_scripts/create_table.json"));
    List<Table> tables = (List<Table>)ObjectMapperTool.getMapper().readValue(
      json, ObjectMapperTool.getMapper().getTypeFactory().constructCollectionType(List.class, Table.class));

    String json2 = IOUtils.toString(
      SchemaMaker.class.getClassLoader().getResourceAsStream("templates/db_scripts/create_view.json"));
    List<View> views = (List<View>)ObjectMapperTool.getMapper().readValue(
      json2, ObjectMapperTool.getMapper().getTypeFactory().constructCollectionType(List.class, View.class));

    fm.setTables(tables);
    fm.setViews(views);
    System.out.println(fm.generateDDL());

  }

  public static String convertDotPath2PostgresNotation(String path){
    String []pathParts = path.split("\\.");
    StringBuilder sb = new StringBuilder("jsonb");
    for (int j = 0; j < pathParts.length; j++) {
      if(j == pathParts.length-1){
        sb.append("->>");
      } else{
        sb.append("->");
      }
      sb.append("'").append(pathParts[j]).append("'");
    }
    return sb.toString();
  }

  public static String convertDotPath2PostgresMutateNotation(String path){
    String []pathParts = path.split("\\.");
    StringBuilder sb = new StringBuilder("'{");
    for (int j = 0; j < pathParts.length; j++) {
      sb.append(pathParts[j]);
      if(j != pathParts.length-1){
        sb.append(",");
      }
    }
    return sb.append("}'").toString();
  }
}
