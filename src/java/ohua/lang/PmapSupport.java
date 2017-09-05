/*
 * ohua : PmapSupport.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.lang;

import ohua.runtime.lang.operator.DataflowFunction;
import ohua.util.Box;
import ohua.util.MutableBox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by justusadam on 25/01/2017.
 */
public final class PmapSupport {
    // should perhaps be immutable at some point
    // private static final Box<Integer> N = ImmutableBox.empty();
    private static final Box<Integer> N = MutableBox.empty();

    static {
        setN(1);
    }

    private PmapSupport() {
    }

    public static int getN() {
        return N.get();
    }

    public static void setN(int n) {
        if (n < 1) throw new IllegalArgumentException("N must be larger 1");
        N.set(n);
    }

    public final static class PackageArgs {

        @defsfn
        public Object[] __packageArgs(final Object... args) {
            return args;
        }
    }

    public final static class Pmap {
        @defsfn
        @DataflowFunction
        public <T> Object[] pmap(final int size, final Collection<T> items) {
            if (items.size() > size)
                throw new IllegalArgumentException("Items collection is too large, expected " + size + " got " + items.size());
            Object[] a = new Object[size];
            items.toArray(a);
            for (int i = items.size(); i < size; i++) {
                a[i] = DataflowFunction.Control.DROP;
            }
            return a;
        }
    }

    public final static class PCollect {
        private static <T> Continuations mkContinuations(final int size, final int index, final NonStrict<T>[] args, final List<T> items) {
            if (index == args.length)
                return Continuations.finish(items);
            else if (index == size)
                return Continuations.empty().handsOff(Arrays.copyOfRange(args, index, args.length));
            else
                return Continuations.empty()
                        .at(args[index], v -> {
                            items.add(v);
                            return mkContinuations(size, index + 1, args, items);
                        });

        }

        @defsfn
        @DataflowFunction
        public <T> Continuations pcollect(final int size, final NonStrict<T>... args) {
            List<T> items = new ArrayList<>(size);
            return mkContinuations(size, 0, args, items);
        }
    }

    public final static class Chunk {

        @defsfn
        public <T> List<List<T>> __chunk(final int chunkSize, final Collection<T> coll) {
            final List<List<T>> out = new ArrayList<>(coll.size() / chunkSize);
            List<T> intermediate = new ArrayList<>(chunkSize);
            for (T object : coll) {
                intermediate.add(object);
                if (intermediate.size() == chunkSize) {
                    out.add(intermediate);
                    intermediate = new ArrayList<>(chunkSize);
                }
            }
            if (intermediate.size() != 0)
                out.add(intermediate);
            return out;
        }
    }

    public final static class Flatten {
        @defsfn
        public <T> List<T> __flatten(final List<Collection<T>> list) {
            int targetSize;
            if (list.size() > 0) {
                targetSize = list.size() * list.get(0).size();
            } else  {
                targetSize = 0;
            }
            final List<T> out = new ArrayList<>(targetSize);
            for (Collection<T> var : list) {
                out.addAll(var);
            }
            return out;
        }
    }

}
