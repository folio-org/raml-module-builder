package org.folio.rest.tools.annotator;

import java.util.Optional;

import org.folio.rest.annotations.ElementsPattern;
import org.folio.rest.annotations.NoNullElements;
import org.jsonschema2pojo.AbstractAnnotator;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JFieldVar;

public class CustomArrayAnnotator extends AbstractAnnotator {

  final private static String REGEXP  = "regexp",
                              TYPE    = "type",
                              ITEMS   = "items",
                              ARRAY   = "array",
                              STRING  = "string",
                              PATTERN = "pattern";

  @Override
  public void propertyField(final JFieldVar field, final JDefinedClass clazz, final String propertyName, final JsonNode propertyNode) {
    super.propertyField(field, clazz, propertyName, propertyNode);
    if(isArray(propertyNode)) {
      field.annotate(NoNullElements.class);
      Optional<String> pattern = getPattern(propertyNode);
      if(pattern.isPresent()) {
        field.annotate(ElementsPattern.class).param(REGEXP, pattern.get());
      }
    }
  }

  private boolean isArray(final JsonNode propertyNode) {
    return propertyNode.has(TYPE) && ARRAY.equals(propertyNode.get(TYPE).asText());
  }

  private Optional<String> getPattern(final JsonNode propertyNode) {
    if (propertyNode.has(ITEMS)) {
      JsonNode itemNode = propertyNode.get(ITEMS);
      if (itemNode.has(TYPE) && STRING.equals(itemNode.get(TYPE).asText()) && itemNode.has(PATTERN)) {
        return Optional.of(itemNode.get(PATTERN).asText());
      }
    }
    return Optional.empty();
  }

}
