package org.folio.rest.tools.utils;

/**
 * @author shale
 *
 */
public class Enum2Annotation {

  private Enum2Annotation(){}

  public static String getAnnotation(String anno){

    switch(anno.toUpperCase()){
      case "PATTERN":
        return "javax.validation.constraints.Pattern";
      case "MIN":
        return "javax.validation.constraints.Min";
      case "MAX":
        return "javax.validation.constraints.Max";
      case "REQUIRED":
        return "javax.validation.constraints.NotNull";
      case "DEFAULTVALUE":
        return "javax.ws.rs.DefaultValue";
      case "SIZE":
        return "javax.validation.constraints.Size";
      default:
          return null;
    }
  }

  public static boolean isVerbEnum(String verb){
    return (verb != null &&
        (verb.equalsIgnoreCase("GET") || verb.equalsIgnoreCase("POST") ||
            verb.equalsIgnoreCase("PUT") || verb.equalsIgnoreCase("DELETE")));
  }
}
