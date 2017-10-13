package ohua.loader;

import ohua.StatefulFunctionProvider;
import java.util.*;
import ohua.runtime.lang.operator.StatefulFunction;

public final class MultiDispatchSFProvider implements StatefulFunctionProvider {
    private final List<StatefulFunctionProvider> dispatchList;

    public MultiDispatchSFProvider(StatefulFunctionProvider[] loaders) {
        this.dispatchList = Arrays.asList(loaders);
    }

    @Override
    public boolean exists(String nsRef, String sfRef) {
        for (StatefulFunctionProvider p : dispatchList) {
            if (p.exists(nsRef, sfRef))
                return true;
        }
        return false;
    }

    @Override
    public StatefulFunction provide(String nsRef, String sfRef) throws Exception {
        for (StatefulFunctionProvider p : dispatchList) {
            StatefulFunction s = p.provide(nsRef, sfRef);
            if (s != null)
                return s;
        }
        return null;
    }

    @Override
    public Iterator<String> list(String nsRef) {
        return new Iter(nsRef);
    }

    final class Iter implements Iterator<String> {
        Iterator<String> currentIt;
        Iterator<StatefulFunctionProvider> others = dispatchList.iterator();
        final String nsRef;

        Iter (String nsRef) {
            this.nsRef = nsRef;
            advance();
        }

        @Override
        public boolean hasNext() {
            return 
                currentIt != null
                && (currentIt.hasNext() || advanceUntilHasNext());
        }

        @Override
        public String next() {
            if (hasNext())
                return currentIt.next();
            else 
                throw new NoSuchElementException();
            
        }

        private boolean advance() {
            if (others.hasNext()) {
                currentIt = others.next().list(nsRef);
                return true;
            }
            return false;

        }

        private boolean advanceUntilHasNext() {
            if (currentIt == null)
                return false;
            while (!currentIt.hasNext()) {
                if (!advance())
                    return false;
            }
            return true;
        }
        
    }

    public static MultiDispatchSFProvider combine(StatefulFunctionProvider... loaders) {
        return new MultiDispatchSFProvider(loaders);
    }

}
