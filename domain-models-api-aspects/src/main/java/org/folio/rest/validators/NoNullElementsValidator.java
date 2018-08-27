package org.folio.rest.validators;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.folio.rest.annotations.NoNullElements;

public class NoNullElementsValidator implements ConstraintValidator<NoNullElements, Collection<?>> {

  @Override
  public void initialize(final NoNullElements noNullElements) {

  }

  @Override
  public boolean isValid(final Collection<?> collection, final ConstraintValidatorContext context) {
    Optional<Collection<?>> items = Optional.ofNullable(collection);
    boolean valid = true;
    if (items.isPresent() && items.get().size() > 0) {
      Iterator<?> iterator = items.get().iterator();
      while (iterator.hasNext()) {
        Optional<Object> element = Optional.ofNullable(iterator.next());
        if (!element.isPresent()) {
          valid = false;
          break;
        }
      }
    }
    return valid;
  }

}
