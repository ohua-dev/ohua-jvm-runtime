package ohua.util;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by sertel on 12/15/16.
 */
public class Util {

  @FunctionalInterface
  public interface ThrowingSupplier<T> extends Supplier<T> {

    @Override
    default T get() {
      try {
        return getThrows();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    T getThrows() throws Exception;
  }

  @FunctionalInterface
  public interface ThrowingFunction<S,T> extends Function<S,T> {

    @Override
    default T apply(S s) {
      try {
        return applyThrows(s);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    T applyThrows(S s) throws Exception;
  }

}
