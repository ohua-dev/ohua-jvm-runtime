package ohua.util;

import java.util.function.Supplier;


/**
 * A lazy is a special type of supplier which executes a computation the first time the `get`
 * method is called and on subsequent calls always returns that constant value.
 */
public interface Lazy<T> extends Supplier<T> {
    public boolean isRealized();

    public static <T> Lazy<T> createLazy(Supplier<T> createValue) {
        return SubclassBasedLazy.createLazy(createValue);
    }

    public static <T> Lazy<T> createRealized(T value) {
        return SubclassBasedLazy.createRealized(value);
    }
}
