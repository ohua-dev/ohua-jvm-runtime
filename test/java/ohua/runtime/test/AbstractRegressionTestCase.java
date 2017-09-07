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

public class AbstractRegressionTestCase extends AbstractOhuaTestCase {
    protected static final String testNS = "test.flow";
    private final static String TEST_ROOT_DIRECTORY = "test/java/";
    private final static String TEST_OUTPUT_DIRECTORY = "test-output/";
    @Rule
    public TestName testName = new TestName();
    // test input
    private String _testClassDirectory = null;
    private String _testMethodDirectory = null;
    // test output
    private String _testClassOutputDirectory = null;
    private String _testMethodOutputDirectory = null;

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

    @Deprecated
    public final String getTestMethodInputDirectory() {
        return _testMethodDirectory;
    }

    @Deprecated
    public final String getTestMethodOutputDirectory() {
        return _testMethodOutputDirectory;
    }

    @Before
    public void regressionSetup() {
        // some test cases might fail or do not call teardown, hence this line makes sure we always
        // start from a clean state id-wise.
        AbstractProcessManager.resetCounters();
    }

    private void regressionClassDirectorySetup() {
        _testClassDirectory = TEST_ROOT_DIRECTORY + getClass().getPackage().getName().replace(".", "/") + "/";
        _testClassOutputDirectory = TEST_OUTPUT_DIRECTORY + getClass().getCanonicalName() + "/";

        createOutputClassDirectory();
    }

    private void createOutputClassDirectory() {
        File outputDir = new File(TEST_OUTPUT_DIRECTORY);
        File outputClassDir = new File(_testClassOutputDirectory);

        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        if (!outputClassDir.exists()) {
            outputClassDir.mkdir();
        }
    }

}
