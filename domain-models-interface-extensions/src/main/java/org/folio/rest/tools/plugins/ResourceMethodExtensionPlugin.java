package org.folio.rest.tools.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.lang.model.element.Modifier;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.Enum2Annotation;
import org.folio.rest.annotations.Validate;
import org.raml.jaxrs.generator.extension.resources.api.ResourceContext;
import org.raml.jaxrs.generator.extension.resources.api.ResourceMethodExtension;
import org.raml.jaxrs.generator.ramltypes.GMethod;
import org.raml.jaxrs.generator.ramltypes.GParameter;
import org.raml.jaxrs.generator.ramltypes.GRequest;
import org.raml.jaxrs.generator.ramltypes.GType;
import org.raml.v2.api.model.v10.datamodel.ExampleSpec;
import org.raml.v2.api.model.v10.datamodel.TypeDeclaration;
import org.raml.v2.api.model.v10.system.types.MarkdownString;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.MethodSpec.Builder;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Iterator;
import org.raml.v2.api.model.v10.datamodel.NumberTypeDeclaration;
import org.raml.v2.api.model.v10.datamodel.StringTypeDeclaration;

/**
 * Used to build vertx methods instead of the standard jaxrs methods that are generated by
 * the raml code generator
 * the class also implements the trait override feature:
 * https://github.com/folio-org/raml-module-builder#overriding-raml-traits--query-parameters
 *
 * /resources/META-INF/ramltojaxrs-plugin.properties should contain the following entry
 *
 * ramltojaxrs.core.one=org.folio.rest.tools.plugins.ResourceMethodExtensionPlugin
 *
 *
 * The GenerateRunner class will activate this plugin by setting the following in the configuration
 *
 * configuration.setTypeConfiguration(new String[]{"core.one"});
 *
 */
public class ResourceMethodExtensionPlugin implements ResourceMethodExtension<GMethod> {

  private static final String ANNOTATION_VALUE = "value";

  private static final Logger log = LoggerFactory.getLogger(ResourceMethodExtensionPlugin.class);

  private static Table<String, String, JsonObject> overrideMap = HashBasedTable.create();
  private static boolean overrideFileExists = true;

  @Override
  public MethodSpec.Builder onMethod(ResourceContext context, GMethod method, GRequest gRequest, MethodSpec.Builder methodSpec) {

    try {
      handleOverrides();
      methodSpec = addAnnotations(method, methodSpec);
      addRoutingContext(methodSpec, method);
      Builder builder = generateOverrides(method.resource().resourcePath(), method.method(), methodSpec);
      addParams(builder);
      generateJavaDocs(method, builder);
      return builder;
    } catch ( Exception e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  private void addRoutingContext(Builder methodSpec, GMethod method) {
    //add routingContext param if indicated in generate runner plugin in pom
    String endpoints2addRoutingContext = PomReader.INSTANCE.getProps().getProperty("generate_routing_context");
    if(endpoints2addRoutingContext != null){
      String []rcFuncs = endpoints2addRoutingContext.split(",");
      for (int i = 0; i < rcFuncs.length; i++) {
        try {
          if(rcFuncs[i].equalsIgnoreCase(method.resource().resourcePath())){
            methodSpec.addParameter(io.vertx.ext.web.RoutingContext.class, "routingContext");
          }
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    }
  }

  private ParameterSpec annotateNew(GParameter get, ParameterSpec orgParam) {
    TypeDeclaration typeDeclaration = (TypeDeclaration) get.implementation();

    ParameterSpec.Builder newParam = cloneSingleParamNoAnnotations(orgParam);
    List<AnnotationSpec> newAnnotations = getAnnotationsAsModifiableList(orgParam);

    if (typeDeclaration instanceof StringTypeDeclaration) {
      StringTypeDeclaration n = (StringTypeDeclaration) typeDeclaration;
      if (n.pattern() != null) {
        AnnotationSpec.Builder annoBuilder = AnnotationSpec.builder(javax.validation.constraints.Pattern.class);
        annoBuilder.addMember("regexp", "$S", n.pattern());
        newAnnotations.add(annoBuilder.build());
      }
    }
    if (typeDeclaration instanceof NumberTypeDeclaration) {
      NumberTypeDeclaration n = (NumberTypeDeclaration) typeDeclaration;
      if (n.minimum() != null) {
        AnnotationSpec.Builder annoBuilder = AnnotationSpec.builder(javax.validation.constraints.Min.class);
        annoBuilder.addMember(ANNOTATION_VALUE, "$L", (Long) n.minimum().longValue());
        newAnnotations.add(annoBuilder.build());
      }
      if (n.maximum() != null) {
        AnnotationSpec.Builder annoBuilder = AnnotationSpec.builder(javax.validation.constraints.Max.class);
        annoBuilder.addMember(ANNOTATION_VALUE, "$L", (Long) n.maximum().longValue());
        newAnnotations.add(annoBuilder.build());
      }
    }
    newParam.addAnnotations(newAnnotations);
    return newParam.build();
  }

  private Builder addAnnotations(GMethod method, MethodSpec.Builder methodSpec) {
    MethodSpec.Builder ret = cloneMethodWithoutParams(methodSpec);

    MethodSpec spec = methodSpec.build();

    List<ParameterSpec> modifiedParams = new ArrayList<>(spec.parameters);
    Iterator<GParameter> methodParams = method.queryParameters().iterator();
    Iterator<GParameter> uriParams = method.resource().uriParameters().iterator();

    for (int j = 0; j < modifiedParams.size(); j++) {
      ParameterSpec orgParam = modifiedParams.get(j);
      List<AnnotationSpec> an = orgParam.annotations;
      for (AnnotationSpec a : an) {
        log.info("a.type.toString=" + a.type.toString());
        if (a.type.toString().equals("javax.ws.rs.QueryParam")) {
          modifiedParams.set(j, annotateNew(methodParams.next(), orgParam));
          break;
        }
        if (a.type.toString().equals("javax.ws.rs.PathParam")) {
          if (uriParams.hasNext()) {
            modifiedParams.set(j, annotateNew(uriParams.next(), orgParam));
          }
          break;
        }
      }
    }
    ret.addParameters(modifiedParams);
    return ret;
  }

  private void addParams(MethodSpec.Builder methodSpec){
    ParameterizedTypeName okapiHeader =
        ParameterizedTypeName.get(ClassName.get(java.util.Map.class),
          ClassName.get(String.class).box(), ClassName.get(String.class).box());
    methodSpec.addParameter(okapiHeader.box(), "okapiHeaders");

    ParameterizedTypeName asyncResult =
        ParameterizedTypeName.get(ClassName.get(io.vertx.core.AsyncResult.class),
          ClassName.get(Response.class).box());

    ParameterizedTypeName asyncHandler =
        ParameterizedTypeName.get(ClassName.get(io.vertx.core.Handler.class),
          asyncResult.box());

    methodSpec.addParameter(asyncHandler.box(), "asyncResultHandler");

    methodSpec.addParameter(io.vertx.core.Context.class, "vertxContext");

    methodSpec.returns(TypeName.VOID);

    methodSpec.addAnnotation(Validate.class);
  }

  private void generateJavaDocs(GMethod method, MethodSpec.Builder methodSpec) {

    final String description = method.getDescription();
    if (description != null) {
      methodSpec.addJavadoc(description + "\n");
    }
    List<GParameter> methodParams = method.queryParameters();

    for (int i = 0; i < methodParams.size(); i++) {
      GParameter get = methodParams.get(i);
      String paramName = get.name();
      methodSpec.addJavadoc("@param " + paramName.replace("\"", ""));
      String defaultVal = get.defaultValue();
      TypeDeclaration typeDeclaration = (TypeDeclaration) get.implementation();
      MarkdownString desc = typeDeclaration.description();
      if (desc != null) {
        methodSpec.addJavadoc("\t" + desc.value());
        methodSpec.addJavadoc("\n");
      }
      if (defaultVal != null) {
        methodSpec.addJavadoc("\tdefault value: $S\n", defaultVal);
      }
      if (typeDeclaration instanceof NumberTypeDeclaration) {
        NumberTypeDeclaration n = (NumberTypeDeclaration) typeDeclaration;
        if (n.minimum() != null) {
          methodSpec.addJavadoc("\tminimum value: $S\n", n.minimum());
        }
        if (n.maximum() != null) {
          methodSpec.addJavadoc("\tmaximum value: $S\n", n.maximum());
        }
      }
      if (typeDeclaration instanceof StringTypeDeclaration) {
        StringTypeDeclaration n = (StringTypeDeclaration) typeDeclaration;
        if (n.pattern() != null) {
          methodSpec.addJavadoc("\tpattern: $S\n", n.pattern());
        }
      }
    }

    List<GRequest> bodyContent = method.body();
    for(int i=0; i< bodyContent.size(); i++){
      GType entity = bodyContent.get(i).type();
      if(entity != null){
        methodSpec.addJavadoc("@param entity <code>"+entity.defaultJavaTypeName("")+"</code>\n");
      }
      ExampleSpec example = ((TypeDeclaration)method.body().get(i).implementation()).example();
      if(example != null){
        methodSpec.addJavadoc(example.value());
        methodSpec.addJavadoc("\n");
      }
    }

    methodSpec.addJavadoc("@param asyncResultHandler An AsyncResult<Response> Handler ");
    methodSpec.addJavadoc(" {@link $T} " , Handler.class);
    methodSpec.addJavadoc("which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.\n");

    methodSpec.addJavadoc("@param vertxContext\n");
    methodSpec.addJavadoc(" The Vertx Context Object <code>io.vertx.core.Context</code> \n");

    methodSpec.addJavadoc("@param okapiHeaders\n");
    methodSpec.addJavadoc(" Case insensitive map of x-okapi-* headers passed in as part of the request <code>java.util.Map<String, String></code> ");
  }

  private static void handleOverrides() throws IOException {
    String overrides = null;
    if(overrideFileExists){
      overrides = IOUtils.toString(ResourceMethodExtensionPlugin.class.getClassLoader().getResourceAsStream(
        "overrides/raml_overrides.json"), "UTF-8");
      if(overrides == null){
        log.info("No overrides/raml_overrides.json file found, continuing...");
        overrideFileExists = false;
        return;
      }
    }
    else{
      return;
    }
    if(overrideMap.isEmpty()){
      try {
        JsonObject jobj = new JsonObject(overrides);
        JsonArray jar = jobj.getJsonArray("overrides");
        int size = jar.size();
        for (int i = 0; i < size; i++) {
          JsonObject overrideEntry = jar.getJsonObject(i);
          String type = overrideEntry.getString("type");
          String url = overrideEntry.getString("url");
          overrideMap.put(url, type, overrideEntry);
        }
      } catch (Exception e1) {
        log.error(e1.getMessage(), e1);
      }
    }
  }

  private MethodSpec.Builder cloneMethodWithoutParams(Builder methodSpec){
    MethodSpec spec = methodSpec.build();
    MethodSpec.Builder newBuilder = MethodSpec.methodBuilder(methodSpec.build().name);
    newBuilder.addAnnotations(spec.annotations);
    newBuilder.addCode(spec.code);
    newBuilder.addExceptions(spec.exceptions);
    newBuilder.addTypeVariables(spec.typeVariables);
    newBuilder.addModifiers(spec.modifiers);
    newBuilder.returns(spec.returnType);
    if(spec.defaultValue != null){
      newBuilder.defaultValue(spec.defaultValue);
    }
    newBuilder.varargs(spec.varargs);
    newBuilder.addCode(spec.javadoc);
    return newBuilder;
  }

  private ParameterSpec.Builder cloneSingleParamNoAnnotations(ParameterSpec spec){
    ParameterSpec.Builder newSpecBuilder = ParameterSpec.builder(spec.type, spec.name);
    Modifier modifiers[] = new Modifier[spec.modifiers.size()];
    newSpecBuilder.addModifiers(spec.modifiers.toArray(modifiers));
    return newSpecBuilder;
  }

  private List<AnnotationSpec> getAnnotationsAsModifiableList(ParameterSpec spec){
    return new ArrayList<>(spec.annotations);
  }

  private Builder generateOverrides(String path, String verb, Builder methodSpec){

    //clone the method without the params as we will need to update params with new / updated
    //annotations and this cannot be done directly on the method as these lists are immutable
    //as java poet allows building code but not editing??
    MethodSpec.Builder ret = cloneMethodWithoutParams(methodSpec);

    //pull out original params and their annotations from here
    MethodSpec spec = methodSpec.build();

    //use this list to remove and then update params whose annotations need changing since
    //the param list on the method in java poet is immutable this is a workaround
    List<ParameterSpec> modifiedParams = new ArrayList<>( spec.parameters );

    //check if url has a param / params associated with it that needs overriding
    Map<String,JsonObject> overrideEntry = overrideMap.row(path);
    if(overrideEntry != null){
      //this endpoint was found in the config file and has an override associated with one of
      //its parameters
      List<ParameterSpec> paramSpec = spec.parameters;
      int []i = new int[]{0};
      for (i[0] = 0; i[0] < paramSpec.size(); i[0]++) {
        //clone the parameter so we can update it
        ParameterSpec.Builder newParam = cloneSingleParamNoAnnotations(paramSpec.get(i[0]));
        List<AnnotationSpec> originalAnnotations = getAnnotationsAsModifiableList(paramSpec.get(i[0]));
        //remove the param, we need to rebuild it and then add it again
        modifiedParams.remove(i[0]);
        Set<Entry<String, JsonObject>> entries = overrideEntry.entrySet();

        entries.forEach( entry -> {
          //iterate through the params of the generated function for this url + verb and look
          //for the parameter whose annotation we need to override
          JsonObject job = entry.getValue();
          String type = job.getString("type");
          Object value = job.getValue(ANNOTATION_VALUE);
          String paramName = job.getString("paramName");
          if(verb.equalsIgnoreCase(job.getString("verb")) &&
            paramName.equalsIgnoreCase(paramSpec.get(i[0]).name)){
            //make sure the verb is aligned
            //we found the parameter that should be overridden, for the path, and for the verb
            //we cannot update the param, so we need to recreate it and then update the list
            //by removing the old and adding the recreated param
            //we need the original annotations so that we can add the ones that were not updated
            for(int j=0; j<originalAnnotations.size(); j++ ){
              if(originalAnnotations.get(j).type.toString() != null
                && Enum2Annotation.getAnnotation(type).endsWith(originalAnnotations.get(j).type.toString())){
                  originalAnnotations.remove(j);
                break;
              }
            }
            try {
              AnnotationSpec aSpec =
                  buildAnnotation(Class.forName(Enum2Annotation.getAnnotation(type)), value, type);
              originalAnnotations.add(aSpec);
            } catch (ClassNotFoundException e) {
              log.error(e.getMessage(), e);
            }
          }
        });
        newParam.addAnnotations(originalAnnotations);
        modifiedParams.add(i[0], newParam.build());
      }
    }
    ret.addParameters(modifiedParams);
    return ret;
  }

  private AnnotationSpec buildAnnotation(Class<?> annotationClass, Object value, String type){

    AnnotationSpec.Builder annoBuilder = AnnotationSpec.builder(annotationClass);

    if(type.equalsIgnoreCase("Size")){
      //size can contain two values (min and max) so it is unlike the other potential annotations
      String []multipleAnnos = ((String)value).split(",");
      for (int j = 0; j < multipleAnnos.length; j++) {
        if(j == 0){
          annoBuilder.addMember("min", "$L", Integer.valueOf(multipleAnnos[j].trim()));
        }
        else{
          annoBuilder.addMember("max", "$L", Integer.valueOf(multipleAnnos[j].trim()));
        }
      }
    }
    else if(!type.equalsIgnoreCase("REQUIRED")){
      //a required annotation will add a @notnull annotation
      //THERE IS CURRENTLY NO SUPPORT TO MAKE A REQUIRED PARAM NOT REQUIRED
      if(value instanceof String){
        if(type.equalsIgnoreCase("PATTERN")){
          annoBuilder.addMember("regexp", "$S", (String)value);
        }
        else{
          annoBuilder.addMember(ANNOTATION_VALUE, "$S", (String)value);
        }
      }
      else if(value instanceof Boolean){
        annoBuilder.addMember(ANNOTATION_VALUE, "$L", (Boolean)value);
      }
      else if(value instanceof Integer){
        annoBuilder.addMember(ANNOTATION_VALUE, "$L", (Integer)value);
      }
    }

    return annoBuilder.build();
  }
}
