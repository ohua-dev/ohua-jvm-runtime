package ohua.loader;

import ohua.StatefulFunctionProvider;
import java.util.Map;
import java.util.HashMap;
import ohua.runtime.lang.operator.StatefulFunction;

public final class MultiDispatchSFProvider implements StatefulFunctionProvider {
    private final StatefulFunctionProvider[] dispatchArray;
    private final Map<String, StatefulFunctionProvider> dispatchCache;

    public MultiDispatchSFProvider(StatefulFunctionProvider[] loaders) {
        this.dispatchArray = loaders;
        dispatchCache = new HashMap<>();
    }

    @Override
    public boolean exists(String sfRef) {
        return dispatchCache.containsKey(sfRef) || fromDispatchers(sfRef);
    }

    @Override
    public StatefulFunction provide(String sfRef) {
        StatefulFunctionProvider loader = dispatchCache.get(sfRef);

        if (loader == null) {
            if (fromDispatchers(sfRef)) 
                loader = dispatchCache.get(sfRef);
            else
                return null;
        }

        StatefulFunction sf = loader.provide(sfRef);

        if (sf == null)
            throw new InvariantBrokenException("StatefulFunctionProvider " + loader + " claimed " + sfRef + " existed but did not provide it.");

        return sf;
    }


    private boolean fromDispatchers(String sfRef) {
        for (StatefulFunctionProvider l: dispatchArray) {
            if (l.exists(sfRef)) {
                dispatchCache.put(sfRef, l);
                return true;
            }
        }

        return false;
    }

    public static MultiDispatchSFProvider combine(StatefulFunctionProvider... loaders) {
        return new MultiDispatchSFProvider(loaders);
    }

    private static final class InvariantBrokenException extends RuntimeException {
        InvariantBrokenException(String msg) {
            super(msg);
        }
    }

}
