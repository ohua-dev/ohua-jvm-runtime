package ohua.util;

import java.util.function.Supplier;

/**
 * This type of lazy works by delegating all calls to an underlying `Realizable` object.
 * Unlike the FlagBasedLazy this has virtual method dispatch (which is bad) but
 * I think it should still be more efficient, because calling `realized` on evaluated values
 * is not `synchronized` anymore.
 */
public final class SubclassBasedLazy<T> implements Lazy<T> {
    private Realizable<T> inner;

    SubclassBasedLazy(Supplier<T> createValue) {
        this.inner = new Unevaluated(createValue);
    }

    SubclassBasedLazy(T value) {
        this.inner = new Evaluated(value);
    }

    private interface Realizable<T> {
        T realize();
        boolean isRealized();
    }

    private final class Unevaluated implements Realizable<T> {
        private final Supplier<T> createValue;

        Unevaluated(Supplier<T> createValue) {
            this.createValue = createValue;
        }

        @Override
        public synchronized T realize() {
            if (inner == this) {
                inner = new Evaluated(createValue.get());
                return inner.realize();
            } else if (inner instanceof SubclassBasedLazy.Evaluated) {
                return inner.realize();
            } else {
                throw new RuntimeException("Invariant broken.");
            }
        }

        @Override
        public boolean isRealized() {
            return false;
        }

    }

    private final class Evaluated implements Realizable<T> {
        private final T value;

        Evaluated(T value) {
            this.value = value;
        }

        @Override
        public T realize() {
            return value;
        }

        @Override
        public boolean isRealized() {
            return true;
        }
    }

    @Override
    public T get() {
        return inner.realize();
    }

    @Override
    public boolean isRealized() {
        return inner.isRealized();
    }

    public static <T> SubclassBasedLazy<T> createLazy(Supplier<T> createValue) {
        return new SubclassBasedLazy<T>(createValue);
    }

    public static <T> SubclassBasedLazy<T> createRealized(T value) {
        return new SubclassBasedLazy<T>(value);
    }

    @Override
    public String toString() {
        if (isRealized()) {
            return "Lazy(" + get().toString() + ")";
        } else {
            return "Lazy(_)";
        }
    }
}
