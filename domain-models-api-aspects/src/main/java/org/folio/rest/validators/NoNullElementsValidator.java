package org.folio.rest.validators;

import java.util.Collection;
import java.util.Iterator;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.folio.rest.annotations.NoNullElements;

public class NoNullElementsValidator implements ConstraintValidator<NoNullElements, Collection<?>> {

  @Override
  public void initialize(final NoNullElements noNullElements) {
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
