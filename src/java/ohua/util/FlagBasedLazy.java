package ohua.util;

import java.util.function.Supplier;


public final class FlagBasedLazy<T> implements Lazy<T> {
    private static final Supplier NO_VALUE_SUPPLIER = new Supplier<Object>() {
        @Override
        public Object get() {
            throw new RuntimeException("This supplier has no value");
        }
    };

    private final Supplier<T> supplier;
    private boolean realized = false;
    private T value = null;

    private FlagBasedLazy(Supplier<T> init) {
        supplier = init;
        realized = false;
        value = null;
    }

    private FlagBasedLazy(T value) {
        supplier = (Supplier<T>) NO_VALUE_SUPPLIER;
        realized = true;
        this.value = value;
    }

    public static <T> Lazy<T> createLazy(Supplier<T> supplier) {
        return new FlagBasedLazy(supplier);
    }

    public static <T> Lazy<T> createRealized(T value) {
        return new FlagBasedLazy(value);
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
    public boolean isRealized() {
        return realized;
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
