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
    private final static String TEST_ROOT_DIRECTORY = "/src/test/";
    private final static String TEST_BASELINE_DIRECTORY = TEST_ROOT_DIRECTORY + "baseline/";
    private final static String TEST_OUTPUT_DIRECTORY = "test-output/";
    @Rule
    public TestName testName = new TestName();
    protected List<FileHandler> _writers = new ArrayList<FileHandler>();
    protected Logger _newLogger = null;
    // test input
    private String _testClassDirectory = null;
    private String _testMethodDirectory = null;
    // test output
    private String _testClassOutputDirectory = null;
    private String _testMethodOutputDirectory = null;
    // test baseline
    private String _testClassBaselineDirectory = null;
    private String _testMethodBaselineDirectory = null;

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

    protected final void outputLogToFile(Logger logger) throws IOException {
        outputLogToFile(logger, Level.ALL);
    }

    protected final void outputLogToFile(String loggerName, Level level) throws IOException {
        outputLogToFile(OhuaLoggerFactory.getLogger(loggerName), level);
    }

    protected final void outputOperatorLogToFile(Class<?> operatorType,
                                                 String operatorName,
                                                 String operatorID,
                                                 Level level) throws IOException {
        outputLogToFile(OhuaLoggerFactory.getLogIDForOperator(operatorType,
                operatorName,
                operatorID), level);
    }

    protected final void outputLogToFile(Logger logger, Level level) throws IOException {
        Level existingLevel = null;
        Logger log = logger;
        while (existingLevel == null) {
            if (log.getLevel() != null) {
                existingLevel = log.getLevel();
            } else {
                log = log.getParent();
            }
        }
        Assertion.invariant(existingLevel != null);
        if (level.intValue() < existingLevel.intValue()) {
            logger.setLevel(level);
        }

        FileHandler writer =
                OhuaLoggingUtils.outputLogToFile(logger, level, getTestMethodOutputDirectory());
        _writers.add(writer);
    }

    protected final void outputLogsToSingleFile(Iterable<String> loggers, Level level) throws IOException {
        FileHandler handler =
                OhuaLoggingUtils.createLogFileHandler("singleFileLogger",
                        level,
                        getTestMethodOutputDirectory());
        handler.setFormatter(new OhuaLoggingFormatter());
        _writers.add(handler);
        for (String loggerID : loggers) {
            Logger logger = OhuaLoggerFactory.getLogger(loggerID);
            logger.addHandler(handler);
        }
    }

    protected final void outputLogToFile(String name) throws IOException {
        // find the logger
        Logger logger = Logger.getLogger(name);
        outputLogToFile(logger);
    }

    protected final void outputLogToFile(Class<?> loggerClass) throws IOException {
        // find the logger
        Logger logger = Logger.getLogger(loggerClass.getCanonicalName());
        outputLogToFile(logger);
    }

    public final String getTestMethodInputDirectory() {
        return _testMethodDirectory;
    }

    public final String getTestClassInputDirectory() {
        return _testClassDirectory;
    }

    public final String getTestMethodOutputDirectory() {
        return _testMethodOutputDirectory;
    }

    public final String getTestClassOutputDirectory() {
        return _testClassOutputDirectory;
    }

    protected final String getTestMethodBaselineDirectoy() {
        return _testMethodBaselineDirectory;
    }

    protected final String getTestClassBaselineDirectoy() {
        return _testClassBaselineDirectory;
    }

    @Before
    public void regressionSetup() {
        regressionClassDirectorySetup();
        regressionMethodDirectorySetup();
        loggerSetup();

        // some test cases might fail or do not call teardown, hence this line makes sure we always
        // start from a clean state id-wise.
        AbstractProcessManager.resetCounters();
    }

    private void loggerSetup() {
        // redirect the logging to System.out and System.err files stored in the output directory of
        // this test case.
        Logger defaultLogger = Logger.getLogger("");
        defaultLogger.setLevel(Level.ALL);
        try {
//            FileHandler logFileHandler =
//                    OhuaLoggingUtils.createLogFileHandler("Console.out",
//                            Level.ALL,
//                            _testMethodOutputDirectory);
//            logFileHandler.setFormatter(new OhuaLoggingFormatter());
//            defaultLogger.addHandler(logFileHandler);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // configure the logger for the test method.
        _newLogger = Logger.getLogger(testName.getMethodName());
        _newLogger.info("************************************ running test: " + testName.getMethodName()
                + " ************************************************");
    }

    private void regressionClassDirectorySetup() {
        String canonicalName = getClass().getCanonicalName();
        String[] packageComponents = canonicalName.split("\\p{Punct}");

        String projectDir = findProjectDirectory();
        String srcFolder = projectDir + File.separator +
                packageComponents[0] + "." + packageComponents[1] + "." + packageComponents[2];
        _testClassDirectory = srcFolder + TEST_ROOT_DIRECTORY + getClass().getSimpleName() + "/";
        _testClassBaselineDirectory =
                srcFolder + TEST_BASELINE_DIRECTORY + getClass().getCanonicalName() + "/";
        _testClassOutputDirectory = TEST_OUTPUT_DIRECTORY + getClass().getCanonicalName() + "/";

        createOutputClassDirectory();
    }

    // TODO this is certainly not the best way to find the root directory!
    private String findProjectDirectory() {
        File f = new File(".");
        Iterator<File> it = org.apache.commons.io.FileUtils.iterateFiles(f, new String[]{"txt"}, true);
        while (it.hasNext()) {
            File c = it.next();
            if (c.getName().equals("LICENSE.txt")) {
                return c.getParent();
            }
        }

        throw new RuntimeException("Project directory not found.");
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
        _testMethodBaselineDirectory = _testClassBaselineDirectory + testName.getMethodName() + "/";

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

    protected final String loadFileContents(String fileName) throws IOException {
        return FileUtils.loadFileContents(new File(getTestMethodInputDirectory() + fileName));
    }
}
