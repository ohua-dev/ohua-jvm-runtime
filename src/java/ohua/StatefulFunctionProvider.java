package ohua;

import ohua.runtime.lang.operator.StatefulFunction;
import ohua.runtime.engine.exceptions.*;

public interface StatefulFunctionProvider {
    boolean exists(String nsRef, String sfRef);
    StatefulFunction provide(String nsRef, String sfRef) throws Exception;
}
