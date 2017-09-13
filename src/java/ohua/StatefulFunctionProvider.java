package ohua;

import ohua.runtime.lang.operator.StatefulFunction;

public interface StatefulFunctionProvider {
    boolean exists(String sfRef);
    StatefulFunction provide(String sfRef);
}
