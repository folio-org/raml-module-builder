package org.folio.rest.persist.ddlgen;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.folio.rest.tools.PomReader;
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
  private TenantOperation mode;
  private String previousVersion;
  private String rmbVersion;
  private Schema schema;

  /**
   * @param onTable
   */
  public SchemaMaker(String tenant, String module, TenantOperation mode, String previousVersion, String rmbVersion){
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
    this.rmbVersion = rmbVersion;
  }

  public String generateDDL() throws IOException, TemplateException {

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

    if(pVersion != null){
      //will be null on deletes unless its read from db by rmb
      int loc = pVersion.lastIndexOf(".");
      if(loc != -1){
        pVersion = this.previousVersion.substring(0, loc);
      }
    }
    else{
      pVersion = "0.0";
    }

    templateInput.put("version", Double.parseDouble(pVersion));
    System.out.println("updating from version " + Double.parseDouble(pVersion));

    templateInput.put("newVersion", PomReader.INSTANCE.getVersion());

    //TODO - check the rmbVersion in the internal_rmb table and compare to this passed in
    //version, to check if core rmb scripts need updating due to an update
    templateInput.put("rmbVersion", this.rmbVersion);

    List<Table> tables = this.schema.getTables();

    Map<String, Index> indexMap = new HashMap<>();

    if(tables != null){
      int size = tables.size();
      for (int i = 0; i < size; i++) {
        Table t = tables.get(i);

        if(t.getMode() == null){
          //the only relevant mode that the templates take into account is delete
          //otherwise update and new will always create if does not exist
          //so can set to either new or update , doesnt matter, leave the option
          //in case we do need to differentiate in the future between the two
          t.setMode("new");
        }

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
            f.setFieldPath(convertDotPath2PostgresNotation(f.getFieldName(), true));
            f.setFieldName(f.getFieldName().replaceAll("\\.", "_"));
          }
        }

        List<Index> ind = t.getIndex();
        if(ind != null){
          for (int j = 0; j < ind.size(); j++) {
            Index ti = ind.get(j);
            if(!ti.isStringType()){
              ti.setCaseSensitive(true);
              ti.setRemoveAccents(false);
            }
            String path = convertDotPath2PostgresNotation(ti.getFieldName(), ti.isStringType());
            ti.setFieldPath(path);
            indexMap.put(t.getTableName()+"_"+ti.getFieldName(), ti);
            ti.setFieldName(ti.getFieldName().replaceAll("\\.", "_"));
          }
        }

        List<Index> tInd = t.getLikeIndex();
        if(tInd != null){
          for (int j = 0; j < tInd.size(); j++) {
            Index ti = tInd.get(j);
            if(!ti.isStringType()){
              ti.setCaseSensitive(true);
              ti.setRemoveAccents(false);
            }
            ti.setFieldPath(convertDotPath2PostgresNotation(ti.getFieldName() , ti.isStringType()));
            ti.setFieldName(ti.getFieldName().replaceAll("\\.", "_"));
          }
        }

        List<Index> gInd = t.getGinIndex();
        if(gInd != null){
          for (int j = 0; j < gInd.size(); j++) {
            Index ti = gInd.get(j);
            ti.setFieldPath(convertDotPath2PostgresNotation(ti.getFieldName() , true));
            ti.setFieldName(ti.getFieldName().replaceAll("\\.", "_"));
          }
        }

        List<Index> uInd = t.getUniqueIndex();
        if(uInd != null){
          for (int j = 0; j < uInd.size(); j++) {
            Index u = uInd.get(j);
            if(!u.isStringType()){
              u.setCaseSensitive(true);
              u.setRemoveAccents(false);
            }
            String path = convertDotPath2PostgresNotation(u.getFieldName(), u.isStringType());
            u.setFieldPath(path);
            //remove . from path since this is incorrect syntax in postgres
            indexMap.put(t.getTableName()+"_"+u.getFieldName(), u);
            u.setFieldName(u.getFieldName().replaceAll("\\.", "_"));
          }
        }
      }
    }

    List<View> views = this.schema.getViews();
    int tablePrefix = 0;
    if(views != null){
      int size = views.size();
      for (int i = 0; i < size; i++) {
        View v = views.get(i);
        //we really only care about deletes from a mode standpoint, since we run CREATE OR REPLACE
        if(v.getMode() == null){
          v.setMode("new");
        }
        List<Join> joins = v.getJoin();
        int jSize = joins.size();
        for (int j = 0; j < jSize; j++) {
          Join join = joins.get(j);
          ViewTable vt = join.getJoinTable();
          vt.setPrefix("p"+tablePrefix++);
          Index index = indexMap.get(vt.getTableName()+"_"+vt.getJoinOnField());
          vt.setJoinOnField(convertDotPath2PostgresNotation( vt.getJoinOnField()  , true));
          if(index != null){
            vt.setIndexUsesCaseSensitive( index.isCaseSensitive() );
            vt.setIndexUsesRemoveAccents( index.isRemoveAccents() );
          }

          vt = join.getTable();
          vt.setPrefix("p"+tablePrefix++);
          index = indexMap.get(vt.getTableName()+"_"+vt.getJoinOnField());
          vt.setJoinOnField(convertDotPath2PostgresNotation( vt.getJoinOnField()  , true));
          if(index != null){
            vt.setIndexUsesCaseSensitive( index.isCaseSensitive() );
            vt.setIndexUsesRemoveAccents( index.isRemoveAccents() );
          }
        }
      }
    }

    templateInput.put("tables", this.schema.getTables());

    templateInput.put("views", this.schema.getViews());

    templateInput.put("scripts", this.schema.getScripts());

    templateInput.put("exactCount", this.schema.getExactCount()+"");

    Template tableTemplate = cfg.getTemplate("main.ftl");
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

  public static String convertDotPath2PostgresNotation(String path, boolean stringType){
    //generate index based on paths - note that all indexes will be with a -> to allow
    //postgres to treat the different data types differently and not ->> which would be all
    //strings
    String []pathParts = path.split("\\.");
    StringBuilder sb = new StringBuilder("jsonb");
    for (int j = 0; j < pathParts.length; j++) {
      if(j == pathParts.length-1){
        if(stringType){
          sb.append("->>");
        }
        else{
          sb.append("->");
        }
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


  public static void main(String args[]) throws Exception {

    SchemaMaker fm = new SchemaMaker("cql5", "mod_inventory_storage", TenantOperation.CREATE, PomReader.INSTANCE.getVersion(), PomReader.INSTANCE.getRmbVersion());
    String f = "C:\\Git\\raml-module-builder\\domain-models-runtime\\src\\main\\resources\\templates\\db_scripts\\examples\\schema.json.example";
    byte[] encoded = Files.readAllBytes(Paths.get(f));
    String json = new String(encoded, "UTF8");

    fm.setSchema(ObjectMapperTool.getMapper().readValue(
      json, Schema.class));

    System.out.println(fm.generateDDL());

  }
}
