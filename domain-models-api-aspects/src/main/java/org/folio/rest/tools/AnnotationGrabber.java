package org.folio.rest.tools;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.folio.rest.tools.utils.ClassPath;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class AnnotationGrabber {
  public static final String  DELIMITER              = "&!!&";
  public static final String  PATH_MAPPING_FILE      = "API_PATH_MAPPINGS";
  public static final String  PATH_ANNOTATION        = Path.class.getName();
  public static final String  INTERFACE_PACKAGE      = "org.folio.rest.jaxrs.resource";
  public static final String  CLASS_NAME             = "class";
  public static final String  INTERFACE_NAME         = "interface";
  public static final String  FUNCTION_NAME          = "function";
  public static final String  CLASS_URL              = "url2class";
  public static final String  METHOD_URL             = "url2method";
  public static final String  REGEX_URL              = "regex2method";
  public static final String  URL_PATH_DELIMITER     = "/";
  public static final String  HTTP_METHOD            = "method";
  public static final String  PRODUCES               = "javax.ws.rs.Produces";
  public static final String  CONSUMES               = "javax.ws.rs.Consumes";
  public static final String  METHOD_PARAMS          = "params";
  public static final String  REPLACEMENT_FROM_REGEX = "/\\{.*?\\}/?";
  public static final String  REPLACEMENT_TO_REGEX   = "\\/([^\\/]+)\\/";
  public static final String  PATH_PARAM             = "@javax.ws.rs.PathParam";
  public static final String  HEADER_PARAM           = "@javax.ws.rs.HeaderParam";
  public static final String  QUERY_PARAM            = "@javax.ws.rs.QueryParam";
  public static final String  DEFAULT_PARAM          = "@javax.ws.rs.DefaultValue";
  public static final String  NON_ANNOTATED_PARAM    = "NON_ANNOTATED";
  public static final String  CONTENT_TYPE           = "Content-Type";

  private static final Logger log = Logger.getLogger(AnnotationGrabber.class.getName());

  // ^http.*?//.*?/apis/patrons/.*?/fines/.*
  // ^http.*?\/\/.*?\/apis\/patrons\/?(.+?)*
  // ^http.*?\/\/.*?\/apis\/([^\/]+)\/([^\/]+)(\?.*)

  public static JsonObject generateMappings(ClientGrabber clientGrabber) throws IOException {
    JsonObject globalClassMapping = new JsonObject();

    // get classes in generated package
    Collection<Class<?>> interfaces = findTopLevelInterfacesInPackage(INTERFACE_PACKAGE);

    // loop over all the classes from the package
    interfaces.forEach(intface -> {
      try {

        // ----------------- class level annotations -----------------------//
        // -----------------------------------------------------------------//

        // will contain all mappings for a specific class in the package
        JsonObject classSpecificMapping = new JsonObject();
        // get annotations via reflection for a class
        Annotation[] annotations = intface.getAnnotations();
        // create an entry for the class name = ex. "class":"com.sling.rest.jaxrs.resource.BibResource"
        classSpecificMapping.put(CLASS_NAME, intface.getName());
        classSpecificMapping.put(INTERFACE_NAME, intface.getName());

        // loop over all the annotations for the class in order to add the
        // needed info - these are class level annotation - not method level
        for (int i = 0; i < annotations.length; i++) {
          // get the annotation type - example in jersey would we javax.ws.rs.Path
          Class<? extends Annotation> type = annotations[i].annotationType();
          //System.out.println("Values of " + type.getName());
          // get the value of this specific annotation - for example /bibs is a
          // possible value for javax.ws.rs.Path - get value by invoking
          // function
          for (Method method : type.getDeclaredMethods()) {
            Object value = method.invoke(annotations[i], (Object[]) null);
            if (type.isAssignableFrom(Path.class)) {
              classSpecificMapping.put(CLASS_URL, "^" + value);
              if (clientGrabber != null) {
                clientGrabber.generateClassMeta(intface.getName());
              }
            }
          }
        }

        // ----------------- method level annotations ------------ //
        // ------------------------------------------------------- //

        JsonArray methodsInAPath;
        // iterate over all functions in the class
        Method[] inputMethods = intface.getMethods();
        // sort generated methods to allow comparing generated file with previous versions
        Arrays.sort(inputMethods, Comparator.comparing(Method::toGenericString));
        for (Method inputMethod : inputMethods) {
          JsonObject methodObj = new JsonObject();

          JsonObject params = getParameterNames(inputMethod);

          // get annotations on the method and add all info per method to its
          // own methodObj
          Annotation[] methodAn = inputMethod.getAnnotations();
          // put the name of the function
          methodObj.put(FUNCTION_NAME, inputMethod.getName());
          methodObj.put(METHOD_PARAMS, params);
          for (int j = 0; j < methodAn.length; j++) {

            Class<? extends Annotation> type = methodAn[j].annotationType();
            if (isPossibleHttpMethod(type.getName())) {
              // put the method - get or post, etc..
              methodObj.put(HTTP_METHOD, type.getName());
            }
            boolean replaceAccept = false;
            if (type.isAssignableFrom(Produces.class)) {
              //this is the accept header, right now can not send */*
              //so if accept header equals any/ - change this to */*
              replaceAccept = true;
            }
            for (Method method : type.getDeclaredMethods()) {
              Object value = method.invoke(methodAn[j], (Object[]) null);
              if (value.getClass().isArray()) {
                List<Object> retList = new ArrayList<>();
                for (int k = 0; k < Array.getLength(value); k++) {
                  if(replaceAccept){
                    //replace any/any with */* to allow declaring accpet */* which causes compilation issues
                    //when declared in raml. so declare any/any in raml instead and replaced here
                    retList.add(((String)Array.get(value, k)).replaceAll("any/any", ""));
                  }
                  else{
                    retList.add(Array.get(value, k));
                  }
                }
                // put generically things like consumes, produces as arrays
                // since they can have multi values
                methodObj.put(type.getName(), retList);
              } else {
                if (type.isAssignableFrom(Path.class)) {
                  String path = classSpecificMapping.getString(CLASS_URL) + value;
                  String regexPath = getRegexForPath(path);
                  // put path to function
                  methodObj.put(METHOD_URL, path);
                  // put regex path to function
                  methodObj.put(REGEX_URL, regexPath);
                }
                //System.out.println(" " + method.getName() + ": " + value.toString());
              }
            }
          }
          // if there was no @Path annotation - use the one declared on the
          // class
          if (methodObj.getString(METHOD_URL) == null) {
            methodObj.put(METHOD_URL, classSpecificMapping.getString(CLASS_URL));
            methodObj.put(REGEX_URL, getRegexForPath(classSpecificMapping.getString(CLASS_URL)));
          }
          if (clientGrabber != null) {
            clientGrabber.generateMethodMeta(methodObj.getString(FUNCTION_NAME),
              methodObj.getJsonObject(METHOD_PARAMS),
              methodObj.getString(METHOD_URL),
              methodObj.getString(HTTP_METHOD),
              methodObj.getJsonArray(CONSUMES),
              methodObj.getJsonArray(PRODUCES));
          }
          // this is the key - the regex path is the key to the functions
          // represented by this url
          // an array of functions which answer to this url (with get, delete,
          // post, etc... methods)
          methodsInAPath = classSpecificMapping.getJsonArray(methodObj.getString(REGEX_URL));
          if (methodsInAPath == null) {
            methodsInAPath = new JsonArray();
            classSpecificMapping.put(methodObj.getString(REGEX_URL), methodsInAPath);
          }
          methodsInAPath.add(methodObj);
        }
        globalClassMapping.put(classSpecificMapping.getString(CLASS_URL), classSpecificMapping);

        if (clientGrabber != null) {
          clientGrabber.generateClass(classSpecificMapping);
        }
      } catch (Exception e) {
        log.log(Level.SEVERE, e.getMessage(), e);
      }
    });
    return globalClassMapping;
  }

  public static JsonObject getParameterNames(Method method) throws Exception {
    // need to handle default values
    JsonObject retObject = new JsonObject();

    Parameter[] nonAnnotationParams = method.getParameters();
    Annotation[][] annotations = method.getParameterAnnotations();
    Class<?>[] parameterTypes = method.getParameterTypes();
    int k = 0;
    for (Annotation[] annotation : annotations) {
      Class<?> parameterType = parameterTypes[k++];
      if (annotation.length == 0) {
        // we are here because - there is a param but it is not annotated - this
        // will occur for post / put
        // requests - the entity to save/update will not be annotated
        JsonObject obj = null;
        obj = new JsonObject();
        obj.put("value", nonAnnotationParams[k - 1].getName()); // this will be
                                                                // a generic
                                                                // name - unless
                                                                // debug info
                                                                // turned on for
                                                                // javac (store
                                                                // information
                                                                // about method
                                                                // parameters)
        obj.put("type", parameterType.getCanonicalName());
        obj.put("order", k - 1);
        obj.put("param_type", NON_ANNOTATED_PARAM);
        retObject.put("" + (k - 1), obj);
      }
      JsonObject prevObjForDefaultVal = null;
      for (Annotation a : annotation) {
        JsonObject obj = null;
        if (a instanceof HeaderParam) {
          obj = new JsonObject();
          obj.put("value", ((HeaderParam) a).value());
          obj.put("type", parameterType.getCanonicalName());
          obj.put("order", k - 1);
          obj.put("param_type", HEADER_PARAM);
        } else if (a instanceof PathParam) {
          obj = new JsonObject();
          obj.put("value", ((PathParam) a).value());
          obj.put("type", parameterType.getCanonicalName());
          obj.put("order", k - 1);
          obj.put("param_type", PATH_PARAM);
        } else if (a instanceof QueryParam) {
          obj = new JsonObject();
          obj.put("value", ((QueryParam) a).value());
          obj.put("type", parameterType.getCanonicalName());
          obj.put("order", k - 1);
          obj.put("param_type", QUERY_PARAM);
        } else if (a instanceof DefaultValue && prevObjForDefaultVal != null) {
          // default values originate in the raml and appear after the parameter
          // they are to be applied to
          String defaultValue = ((DefaultValue) a).value();
          // push it into the previously scanned parameter
          prevObjForDefaultVal.put("default_value", defaultValue);
          prevObjForDefaultVal = null;
        }
        if (obj != null) {
          prevObjForDefaultVal = obj;
          // obj may be null in case of @DefaultValue annotation which i am
          // currently ignoring
          retObject.put("" + (k - 1), obj);
        }
      }
    }
    return retObject;
  }

  private static Collection<Class<?>> findTopLevelInterfacesInPackage(String packageName) throws IOException {
    ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());

    List<Class<?>> result = new ArrayList<>();

    Set<ClassPath.ClassInfo> classInfo = classPath.getTopLevelClasses(packageName);
    for (ClassPath.ClassInfo info : classInfo) {
      Class<?> cl = info.load();

      if (cl.isInterface()) {
        result.add(cl);
      }
    }

    return result;
  }

  private static boolean isPossibleHttpMethod(String method) {
    switch (method) {
    case "javax.ws.rs.PUT":
    case "javax.ws.rs.POST":
    case "javax.ws.rs.DELETE":
    case "javax.ws.rs.GET":
    case "javax.ws.rs.OPTIONS":
    case "javax.ws.rs.HEAD":
    case "javax.ws.rs.TRACE":
    case "javax.ws.rs.CONNECT":
    case "org.folio.rest.jaxrs.resource.support.OPTIONS":
    case "org.folio.rest.jaxrs.resource.support.PATCH":
      return true;
    default:
      return false;
    }
  }

  private static String getRegexForPath(String path) {
    // fix this hack - by writing a better regex
    String regexPath = path.replaceAll(REPLACEMENT_FROM_REGEX, REPLACEMENT_TO_REGEX);
    // regexPath = regexPath.replaceAll(REPLACEMENT_FROM_REGEX,
    // REPLACEMENT_TO_REGEX);

    if (regexPath.endsWith(URL_PATH_DELIMITER)) {
      regexPath = regexPath.concat("?");
    } else {
      regexPath = regexPath.concat("/?");
    }
    regexPath = regexPath.concat("$");
    return regexPath;
  }



}
