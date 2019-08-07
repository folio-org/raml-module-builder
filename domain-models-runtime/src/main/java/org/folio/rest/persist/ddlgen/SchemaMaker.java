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

    FullText fullText = this.schema.getFullText();

    String defaultDictionary = FullText.DEFAULT_DICTIONARY;

    if(fullText != null && fullText.getDefaultDictionary() != null){
      defaultDictionary = fullText.getDefaultDictionary();
    }

    templateInput.put("ft_defaultDictionary", defaultDictionary);

    //TODO - check the rmbVersion in the internal_rmb table and compare to this passed in
    //version, to check if core rmb scripts need updating due to an update
    templateInput.put("rmbVersion", this.rmbVersion);

    List<Table> tables = this.schema.getTables();

    Map<String, Index> indexMap = new HashMap<>();

    if(tables != null){
      for (Table t : tables) {
        if(t.getMode() == null) {
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
            //NOTE , FK are created on fields without the lowercasing / unaccenting
            //meaning, there needs to be an index created without lowercasing / unaccenting
            //otherwise no index will be used
            ForeignKeys f = fKeys.get(j);
            f.setFieldPath(convertDotPath2PostgresNotation("NEW",f.getFieldName(), true , null, false));
            f.setFieldName(normalizeFieldName(f.getFieldName()));
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
            String path = convertDotPath2PostgresNotation(null,ti.getFieldName(), ti.isStringType() , ti, false);
            ti.setFieldPath(path);
            indexMap.put(t.getTableName()+"_"+normalizeFieldName(ti.getFieldName()), ti);
            ti.setFieldName(normalizeFieldName(ti.getFieldName()));
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
            ti.setFieldPath(convertDotPath2PostgresNotation(null,ti.getFieldName() , ti.isStringType(), ti, true));
            ti.setFieldName(normalizeFieldName(ti.getFieldName()));
          }
        }

        List<Index> gInd = t.getGinIndex();
        if(gInd != null){
          for (int j = 0; j < gInd.size(); j++) {
            Index ti = gInd.get(j);
            ti.setFieldPath(convertDotPath2PostgresNotation(null,ti.getFieldName() , true , ti, true));
            ti.setFieldName(normalizeFieldName(ti.getFieldName()));
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
            String path = convertDotPath2PostgresNotation(null,u.getFieldName(), u.isStringType(), u, false);
            u.setFieldPath(path);
            String normalized = normalizeFieldName(u.getFieldName());
            //remove . from path since this is incorrect syntax in postgres
            indexMap.put(t.getTableName()+"_"+normalized, u);
            u.setFieldName(normalized);
          }
        }

        List<Index> ftInd = t.getFullTextIndex();
        if(ftInd != null){
          for (int j = 0; j < ftInd.size(); j++) {
            Index u = ftInd.get(j);
            if (u.isCaseSensitive()) {
              throw new IllegalArgumentException("full text index does not support case sensitive: "
                  + t.getTableName() + " " + u.getFieldName());
            }
            String path = convertDotPath2PostgresNotation(null,u.getFieldName(), true, u, true);
            u.setFieldPath(path);
            //remove . from path since this is incorrect syntax in postgres
            String normalized = normalizeFieldName(u.getFieldName());
            indexMap.put(t.getTableName()+"_"+normalized, u);
            u.setFieldName(normalized);
          }
        }

        if (t.isWithAuditing()) {
          if (t.getAuditingTableName() == null) {
            throw new IllegalArgumentException(
                "auditingTableName missing for table " + t.getTableName() + " having \"withAuditing\": true");
          }
          if (t.getAuditingFieldName() == null) {
            throw new IllegalArgumentException(
                "auditingFieldName missing for table " + t.getTableName() + " having \"withAuditing\": true");
          }
        }
      }
    }

    List<View> views = this.schema.getViews();
    if(views != null){
      int size = views.size();
      for (int i = 0; i < size; i++) {
        View v = views.get(i);
        //we really only care about deletes from a mode standpoint, since we run sql statements with "CREATE OR REPLACE"
        if(v.getMode() == null){
          v.setMode("new");
        }
        List<Join> joins = v.getJoin();
        int jSize = joins.size();
        for (int j = 0; j < jSize; j++) {
          Join join = joins.get(j);
          ViewTable vt = join.getJoinTable();
          vt.setPrefix(vt.getTableName());
          Index index = indexMap.get(vt.getTableName()+"_"+normalizeFieldName(vt.getJoinOnField()));
          vt.setJoinOnField(convertDotPath2PostgresNotation(vt.getPrefix(),
            vt.getJoinOnField(), true, index, false));
          if(index != null){
          //when creating the join on condition, we want to create it the same way as we created the index
          //so that the index will get used, for example:
          //ON lower(f_unaccent(instance.jsonb->>'id'::text))=lower(f_unaccent(holdings_record.jsonb->>'instanceId'))
            vt.setIndexUsesCaseSensitive( index.isCaseSensitive() );
            vt.setIndexUsesRemoveAccents( index.isRemoveAccents() );
          }

          vt = join.getTable();
          vt.setPrefix(vt.getTableName());
          index = indexMap.get(vt.getTableName()+"_"+normalizeFieldName(vt.getJoinOnField()));
          vt.setJoinOnField( convertDotPath2PostgresNotation(vt.getPrefix(),
            vt.getJoinOnField() , true, index, false));
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

  private static String normalizeFieldName(String path) {
    return path.replaceAll("\\.", "_").replaceAll(",","_").replaceAll(" ", "");
  }

  /**
   * Convert JSON dot path to PostgreSQL notation. By default string type index will be
   * wrapped with lower/f_unaccent functions except full text index <code>(isFtIndex = true)</code>.
   * Full text index uses to_tsvector to normalize token, so no need of lower/f_unaccent.
   *
   * @param prefix
   * @param path
   * @param stringType
   * @param index
   * @param isFullText
   * @return
   */
  private static String convertDotPath2PostgresNotation(String prefix,
    String path, boolean stringType, Index index, boolean isFullText){
    //when an index is on multiple columns, this will be defined something like "username,type"
    //so split on command and build a path for each and then combine
    String []requestIndexPath = path.split(",");
    StringBuilder finalClause = new StringBuilder();
    for (int i = 0; i < requestIndexPath.length; i++) {
      if(finalClause.length() > 0) {
        if(isFullText) {
          finalClause.append(" || ' ' || ");
        }
        else {
          finalClause.append(" , ");
        }
      }
      //generate index based on paths - note that all indexes will be with a -> to allow
      //postgres to treat the different data types differently and not ->> which would be all
      //strings
      String []pathParts = requestIndexPath[i].trim().split("\\.");
      String prefixString = "jsonb";
      if(prefix != null) {
        prefixString = prefix +".jsonb";
      }
      StringBuilder sb = new StringBuilder(prefixString);
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
      boolean added = false;
      if (index != null && stringType) {
        if (index.isRemoveAccents()) {
          sb.insert(0, "f_unaccent(").append(")");
          added = true;
        }
        if (!index.isCaseSensitive()) {
          sb.insert(0, "lower(").append(")");
          added = true;
        }
      }
      if (!added) {
        //need to wrap path expression in () if lower / unaccent isnt
        //appended to the path
        sb.insert(0, "(").append(")");
      }
      finalClause.append(sb.toString());
    }
    return finalClause.toString();
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

    SchemaMaker fm = new SchemaMaker("cql7", "mod_inventory_storage", TenantOperation.UPDATE, "mod-foo-18.2.1-SNAPSHOT.2", "mod-foo-18.2.4-SNAPSHOT.2");
    String f = "C:\\Git\\clones\\rmb_release\\master\\raml-module-builder\\domain-models-runtime\\src\\main\\resources\\templates\\db_scripts\\examples\\schema.json.example.json";
    byte[] encoded = Files.readAllBytes(Paths.get(f));
    String json = new String(encoded, "UTF8");
    fm.setSchema(ObjectMapperTool.getMapper().readValue(
      json, Schema.class));
    System.out.println(fm.generateDDL());
  }
}
