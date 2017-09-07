/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.test;

import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.lang.JavaSFNLinker;
import ohua.runtime.lang.OhuaFrontend;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.lang.reflect.Method;

public class AbstractRegressionTestCase {
    protected static final String testNS = "test.flow";
    @Rule
    public TestName testName = new TestName();

    protected static void clearCache() {
      JavaSFNLinker.clear();
    }

    protected static void registerFunc(String name, Method handle) {
      JavaSFNLinker.registerFunction(testNS, name, handle);
    }

    protected static void createOp(OhuaFrontend runtime, String name, int id) throws Throwable {
        runtime.createOperator(testNS + "/" + name, id);
    }

    protected static void loadCoreOps() {
      JavaSFNLinker.loadCoreOperators();
    }

    @Before
    public void regressionSetup() {
        // some test cases might fail or do not call teardown, hence this line makes sure we always
        // start from a clean state id-wise.
        AbstractProcessManager.resetCounters();
    }

}
