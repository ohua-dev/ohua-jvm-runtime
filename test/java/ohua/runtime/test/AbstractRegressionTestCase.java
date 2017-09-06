/*
 * Copyright (c) Sebastian Ertel 2008-2009. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.test;

import ohua.link.JavaBackendProvider;
import ohua.runtime.engine.AbstractProcessManager;
import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.utils.FileUtils;
import ohua.runtime.engine.utils.OhuaLoggerFactory;
import ohua.runtime.engine.utils.OhuaLoggingFormatter;
import ohua.runtime.engine.utils.OhuaLoggingUtils;
import ohua.runtime.lang.OhuaFrontend;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AbstractRegressionTestCase extends AbstractOhuaTestCase {
    protected static final String testNS = "test.flow";
    private final static String TEST_ROOT_DIRECTORY = "test/java/";
    private final static String TEST_OUTPUT_DIRECTORY = "test-output/";
    @Rule
    public TestName testName = new TestName();
    protected List<FileHandler> _writers = new ArrayList<>();
    // test input
    private String _testClassDirectory = null;
    private String _testMethodDirectory = null;
    // test output
    private String _testClassOutputDirectory = null;
    private String _testMethodOutputDirectory = null;

    protected static void clearCache() {
        JavaBackendProvider.clearCache();
    }

    protected static void registerFunc(String name, Method handle) {
        JavaBackendProvider.registerFunction(testNS, name, handle);
    }

    protected static void createOp(OhuaFrontend runtime, String name, int id) throws Throwable {
        runtime.createOperator(testNS + "/" + name, id);
    }

    protected static void loadCoreOps() {
        JavaBackendProvider.loadCoreOperators();
    }

    @Deprecated
    public final String getTestMethodInputDirectory() {
        return _testMethodDirectory;
    }

    public final String getTestClassInputDirectory() {
        return _testClassDirectory;
    }

    @Deprecated
    public final String getTestMethodOutputDirectory() {
        return _testMethodOutputDirectory;
    }

    @Before
    public void regressionSetup() {
        regressionClassDirectorySetup();
        regressionMethodDirectorySetup();

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

    private void regressionMethodDirectorySetup() {
        _testMethodDirectory = _testClassDirectory + testName.getMethodName() + "/";
        _testMethodOutputDirectory = _testClassOutputDirectory + testName.getMethodName() + "/";

        createOutputMethodDirectory();
    }

    private void createOutputMethodDirectory() {
        File outputMethodDir = new File(_testMethodOutputDirectory);
        if (outputMethodDir.exists()) {
            FileUtils.cleanupDirectory(outputMethodDir);
        } else {
            boolean success = outputMethodDir.mkdir();
            assert success;
        }
    }

    @After
    public void closeFileWriters() {
        for (FileHandler handler : _writers) {
            handler.flush();
            handler.close();
        }

        // take care of the default logger
        Logger defaultLogger = Logger.getLogger("");
        List<Handler> handlers = Arrays.asList(defaultLogger.getHandlers());
        for (Handler handler : handlers) {
            if (handler instanceof FileHandler) {
                defaultLogger.removeHandler(handler);
                handler.flush();
                handler.close();
            }
        }
    }
}
