package ohua.util;

import java.util.function.Consumer;
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

  @FunctionalInterface
  public interface ThrowingConsumer<S> extends Consumer<S> {

    @Override
    default void accept(S s) {
      try {
        acceptThrows(s);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    void acceptThrows(S s) throws Exception;
  }

  @FunctionalInterface
  public interface ThrowingRunnable extends Runnable {

    @Override
    default void run() {
      try {
        runThrows();
      } catch (final Throwable e) {
        throw new RuntimeException(e);
      }
    }

    void runThrows() throws Throwable;
  }

}
