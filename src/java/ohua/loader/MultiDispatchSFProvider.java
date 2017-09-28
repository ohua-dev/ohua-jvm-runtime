package ohua.loader;

import ohua.StatefulFunctionProvider;
import java.util.Map;
import java.util.HashMap;
import ohua.runtime.lang.operator.StatefulFunction;

public final class MultiDispatchSFProvider implements StatefulFunctionProvider {
    private final StatefulFunctionProvider[] dispatchArray;
    private final Map<String, Map<String,StatefulFunctionProvider>> dispatchCache;

    public MultiDispatchSFProvider(StatefulFunctionProvider[] loaders) {
        this.dispatchArray = loaders;
        dispatchCache = new HashMap<>();
    }

    @Override
    public boolean exists(String nsRef, String sfRef) {
        return containsKey(nsRef, sfRef) || fromDispatchers(nsRef, sfRef);
    }

    private boolean containsKey(String nsRef, String sfRef) {
        return get(nsRef, sfRef) != null;
    }

    private StatefulFunctionProvider get(String nsRef, String sfRef) {
        Map<String,StatefulFunctionProvider> m2 = dispatchCache.get(nsRef);
        if (!(m2==null))
            return m2.get(sfRef);
        return null;
    }

    private void put(String nsRef, String sfRef, StatefulFunctionProvider sf) {
        Map<String,StatefulFunctionProvider> m2 = dispatchCache.get(nsRef);
        if (m2==null) {
            m2 = new HashMap<>();
            dispatchCache.put(nsRef, m2);
        }
        m2.put(sfRef, sf);
    }

    @Override
    public StatefulFunction provide(String nsRef, String sfRef) throws Exception {
        StatefulFunctionProvider loader = get(nsRef, sfRef);

        if (loader == null) {
            if (fromDispatchers(nsRef, sfRef))
                loader = get(nsRef, sfRef);
            else
                return null;
        }

        StatefulFunction sf = loader.provide(nsRef, sfRef);

        if (sf == null)
            throw new InvariantBrokenException("StatefulFunctionProvider " + loader + " claimed " + sfRef + " existed but did not provide it.");

        return sf;
    }


    private boolean fromDispatchers(String nsRef, String sfRef) {
        for (StatefulFunctionProvider l: dispatchArray) {
            if (l.exists(nsRef, sfRef)) {
                put(nsRef, sfRef, l);
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
