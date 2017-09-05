/*
 * ohua : LazyChunkedIterableWrapper.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by justusadam on 07/02/2017.
 */
public class LazyChunkedIterableWrapper<T> implements Iterable<List<T>> {
    private final int chunkSize;
    private final Iterable<T> innerIterable;

    public LazyChunkedIterableWrapper(int chunkSize, Iterable<T> innerIterable) {
        this.chunkSize = chunkSize;
        this.innerIterable = innerIterable;
    }

    @Override
    public Iterator<List<T>> iterator() {
        return new LazyChunkedIterableWrapperIterator(innerIterable.iterator());
    }

    private class LazyChunkedIterableWrapperIterator implements Iterator<List<T>> {
        private final Iterator<T> innerIterator;

        LazyChunkedIterableWrapperIterator(Iterator<T> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public List<T> next() {
            List<T> l = new ArrayList<T>(chunkSize);
            for (int i = 0; i < chunkSize && innerIterator.hasNext(); i++) {
                l.add(innerIterator.next());
            }
            return l;
        }
    }
}
