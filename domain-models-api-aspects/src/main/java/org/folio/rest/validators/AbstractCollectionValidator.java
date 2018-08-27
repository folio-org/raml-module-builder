package org.folio.rest.validators;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

import javax.validation.ConstraintValidatorContext;

public abstract class AbstractCollectionValidator<A extends Annotation, E, C extends Collection<E>>
    implements CollectionValidator<A, E, C> {

  @Override
  public boolean isValid(final C collection, final ConstraintValidatorContext context) {
    Optional<C> items = Optional.ofNullable(collection);
    boolean valid = true;
    if (items.isPresent() && items.get().size() > 0) {
      Iterator<E> iterator = items.get().iterator();
      while (iterator.hasNext()) {
        Optional<E> element = Optional.ofNullable(iterator.next());
        if (!isEntityValid(element)) {
          valid = false;
          break;
        }
      }
    }
    return valid;
  }

}
