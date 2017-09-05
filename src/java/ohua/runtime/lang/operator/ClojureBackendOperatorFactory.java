/*
 * ohua : ClojureBackendOperatorFactory.java
 *
 * Copyright (c) Sebastian Ertel, Justus Adam 2016. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.lang.operator;

import ohua.runtime.engine.flowgraph.elements.FlowGraph;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperator;
import ohua.runtime.engine.flowgraph.elements.operator.UserOperatorAdapter;

import java.lang.reflect.Method;

/**
 * Created by justusadam on 02/12/2016.
 */
public final class ClojureBackendOperatorFactory {
    private ClojureBackendOperatorFactory() {
    }

    @SuppressWarnings("unchecked")
    public static UserOperator createUserOperatorInstance(Method sf) throws Throwable {
        Class<?> clz = sf.getDeclaringClass();
        if (UserOperator.class.isAssignableFrom(clz)) {
            return (UserOperator) clz.newInstance();
        } else {
            Object func = StatefulFunction.createStatefulFunctionObject(clz);
            AbstractFunctionalOperator op =
                    sf.isAnnotationPresent(DataflowFunction.class) ?
                            new FunctionalDataflowOperator() :
                            new FunctionalOperator();
            op.setFunctionObject(func);
            return op;
        }
    }

    public static OperatorCore createUserOperatorCore(FlowGraph graph, Method mRef, String opName) throws Throwable {
        UserOperator operator = createUserOperatorInstance(mRef);
        OperatorCore core = new OperatorCore(opName);
        graph.addOperator(core);
        UserOperatorAdapter adapter = new UserOperatorAdapter(core, operator);
        core.setOperatorAdapter(adapter);
        operator.setOperatorAlgorithmAdapter(adapter);
        return core;
    }

}
