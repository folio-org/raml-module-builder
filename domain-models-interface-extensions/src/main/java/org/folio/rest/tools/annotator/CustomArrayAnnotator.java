package org.folio.rest.tools.annotator;

import java.util.Optional;

import org.folio.rest.annotations.ElementsPattern;
import org.folio.rest.annotations.NoNullElements;
import org.jsonschema2pojo.AbstractAnnotator;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JFieldVar;

public class CustomArrayAnnotator extends AbstractAnnotator {

  @Override
  public void propertyField(JFieldVar field, JDefinedClass clazz, String propertyName, JsonNode propertyNode) {
    super.propertyField(field, clazz, propertyName, propertyNode);
    Optional<JsonNode> typeNode = Optional.ofNullable(propertyNode.get("type"));
    if (typeNode.isPresent() && typeNode.get().asText().equals("array")) {
      field.annotate(NoNullElements.class);
      Optional<JsonNode> itemsNode = Optional.ofNullable(propertyNode.get("items"));
      if (itemsNode.isPresent()) {
        Optional<JsonNode> itemsTypeNode = Optional.ofNullable(itemsNode.get().get("type"));
        Optional<JsonNode> patternNode = Optional.ofNullable(itemsNode.get().get("pattern"));
        if (itemsTypeNode.isPresent() && itemsTypeNode.get().asText().equals("string") && patternNode.isPresent()) {
          field.annotate(ElementsPattern.class).param("regexp", patternNode.get().asText());
        }
      }
    }
  }

}
