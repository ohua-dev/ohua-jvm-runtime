/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.test;

import ohua.loader.JavaProviderFromAnnotatedMethod;
import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorFactory;
import ohua.runtime.lang.OhuaFrontend;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.lang.reflect.Method;

public class AbstractRegressionTestCase {
    protected static final String testNS = "test.flow";
    @Rule
    public TestName testName = new TestName();
    private static JavaProviderFromAnnotatedMethod provider = new JavaProviderFromAnnotatedMethod();

    protected static void clearCache() {

    }

    protected static void registerFunction(String ns, String name, Method handle) {
        OperatorFactory.registerUserOperator(ns + "/" + name, handle.getDeclaringClass().getName(), true);
    }

    protected static void registerFunction(String name, Method handle) {
        registerFunction(testNS, name, handle);
    }

    protected static void createOp(OhuaFrontend runtime, String name, int id) throws Throwable {
        runtime.createOperator(testNS + "/" + name, id);
    }

    protected static void loadCoreOps() {
        provider.tryLoadNS("ohua.lang");
    }

    @Before
    public void regressionSetup() {
        // some test cases might fail or do not call teardown, hence this line makes sure we always
        // start from a clean state id-wise.
        AbstractProcessManager.resetCounters();
    }

}
