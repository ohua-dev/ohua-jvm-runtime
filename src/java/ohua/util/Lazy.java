package ohua.util;

import java.util.function.Supplier;


public final class Lazy<T> implements Supplier<T> {
    private static final Supplier NO_VALUE_SUPPLIER = new Supplier<Object>() {
        @Override
        public Object get() {
            throw new RuntimeException("This supplier has no value");
        }
    };

    private final Supplier<T> supplier;
    private boolean realized = false;
    private T value = null;

    private Lazy(Supplier<T> init) {
        supplier = init;
        realized = false;
        value = null;
    }

    private Lazy(T value) {
        supplier = (Supplier<T>) NO_VALUE_SUPPLIER;
        realized = true;
        this.value = value;
    }

    public static <T> Lazy<T> createLazy(Supplier<T> supplier) {
        return new Lazy(supplier);
    }

    public static <T> Lazy<T> createRealized(T value) {
        return new Lazy(value);
    }

    private synchronized T realize() {
        if (realized) {
            return value;
        } else if (value != null) {
            throw new RuntimeException("INVARIANT BROKEN: unrealized lazy value had non-null value.");
        } else {
            value = supplier.get();
            realized = true;
            return value;
        }
    }

    @Override
    public T get() {
        return realize();
    }

    @Override
    public String toString() {
        if (realized) {
            return "Lazy(" + value.toString() + ")";
        } else {
            return "Lazy(_)";
        }
    }

}
