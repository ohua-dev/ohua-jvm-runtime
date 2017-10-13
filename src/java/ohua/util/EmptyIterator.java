package ohua.util;

import java.util.NoSuchElementException;
import java.util.Iterator;

public final class EmptyIterator<T> implements Iterator<T> {
    private final static EmptyIterator i = new EmptyIterator();
    
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException("Empty iterator has no elements");
    }

    public static final <T> EmptyIterator<T> it() {
        return (EmptyIterator<T>) i;
    }
}
