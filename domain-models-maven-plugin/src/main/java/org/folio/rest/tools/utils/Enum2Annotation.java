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
        return "jakarta.validation.constraints.Pattern";
      case "MIN":
        return "jakarta.validation.constraints.Min";
      case "MAX":
        return "jakarta.validation.constraints.Max";
      case "REQUIRED":
        return "jakarta.validation.constraints.NotNull";
      case "DEFAULTVALUE":
        return "javax.ws.rs.DefaultValue";
      case "SIZE":
        return "jakarta.validation.constraints.Size";
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
