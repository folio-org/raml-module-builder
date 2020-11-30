package org.folio.rest.tools.utils;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class GenerateRoutingContext {
  // pom.xml copies this file to target/generated-sources and fills in the value from pom.xml <properties>
  private static final String GENERATE_ROUTING_CONTEXT_STRING = "${generate_routing_context}";
  private static final Set<String> GENERATE_ROUTING_CONTEXT = string2set(GENERATE_ROUTING_CONTEXT_STRING);
  
  /**
   * All paths for which RMB should generated a RoutingContext.
   */
  public static Set<String> enabled() {
    return GENERATE_ROUTING_CONTEXT;
  }
  
  static Set<String> string2set(String nameList) {
    return Arrays.asList(nameList.split(","))
        .stream()
        .map(String::trim)
        .filter(name -> ! name.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }
}
