package co.actioniq.ivy.s3;

import java.util.Optional;
import java.util.function.Supplier;

class Optionals {
  private Optionals() {}

  @SafeVarargs
  static <T> Optional<T> first(Supplier<Optional<T>>... suppliers) {
    for (Supplier<Optional<T>> supplier : suppliers) {
      Optional<T> optional = supplier.get();
      if (optional.isPresent()) {
        return optional;
      }
    }
    return Optional.empty();
  }

  @SafeVarargs
  static <T> Optional<T> first(Optional<T>... optionals) {
    for (Optional<T> optional : optionals) {
      if (optional.isPresent()) {
        return optional;
      }
    }
    return Optional.empty();
  }
}
