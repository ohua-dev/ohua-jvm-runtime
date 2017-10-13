package ohua;

import ohua.runtime.lang.operator.StatefulFunction;
import ohua.runtime.engine.exceptions.*;
import java.util.Iterator;

public interface StatefulFunctionProvider {
    public boolean exists(String nsRef, String sfRef);
    public StatefulFunction provide(String nsRef, String sfRef) throws Exception;
    public Iterator<String> list(String nsRef);
}
