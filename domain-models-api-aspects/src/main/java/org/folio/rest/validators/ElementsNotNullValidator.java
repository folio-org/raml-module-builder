package org.folio.rest.validators;

import java.util.Collection;
import java.util.Iterator;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import org.folio.rest.annotations.ElementsNotNull;

public class ElementsNotNullValidator implements ConstraintValidator<ElementsNotNull, Collection<?>> {

  @Override
  public void initialize(final ElementsNotNull elementsNotNull) {
    // Nothing to do here.
  }

  @Override
  public boolean isValid(final Collection<?> collection, final ConstraintValidatorContext context) {
    if (collection != null && !collection.isEmpty()) {
      Iterator<?> iterator = collection.iterator();
      while (iterator.hasNext()) {
        if (iterator.next() == null) {
          return false;
        }
      }
    }
    return true;
  }

}
