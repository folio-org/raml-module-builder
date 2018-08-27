package org.folio.rest.validators;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;

import org.folio.rest.annotations.ElementsPattern;

public class ElementsPatternValidator extends AbstractCollectionValidator<ElementsPattern, String, Collection<String>> {

  private Pattern pattern;

  @Override
  public void initialize(final ElementsPattern elementsPattern) {
    pattern = Pattern.compile(elementsPattern.regexp());
  }

  @Override
  public boolean isEntityValid(Optional<String> element) {
    return element.isPresent() && pattern.matcher(element.get()).matches();
  }

}
