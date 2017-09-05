/*
 * Copyright (c) Sebastian Ertel 2015. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import ohua.runtime.test.AbstractRegressionTestCase;
import org.junit.Assert;
import org.junit.Test;

import ohua.runtime.engine.utils.FileUtils;

public class testClasspathFileLoading extends AbstractRegressionTestCase {
  
  /**
   * The class loader just always adds the URLs into the set of discovered resources.
   * @author sertel
   *
   */
  public static class TestURLClassLoader extends URLClassLoader {
    private URL[] _urls = null;
    
    public TestURLClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
      _urls = urls;
    }
    
    public Enumeration<URL> findResources(final String name) throws IOException {
      final Enumeration<URL> urls = super.findResources(name);
      
      final List<URL> matchedURLs = new ArrayList<>();
      for(URL url : _urls)
        if(url.toString().contains(name)) matchedURLs.add(url);
      
      return new Enumeration<URL>() {
        @Override
        public boolean hasMoreElements() {
          return urls.hasMoreElements() || !matchedURLs.isEmpty();
        }
        
        @Override
        public URL nextElement() {
          return urls.hasMoreElements() ? urls.nextElement() : matchedURLs.remove(0);
        }
      };
    }
        
    public static void addToClasspath(URL... references) throws MalformedURLException {
      ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
      
      // Add the conf dir to the classpath
      // Chain the current thread classloader
      URLClassLoader urlClassLoader =
          new TestURLClassLoader(references,
                                 currentThreadClassLoader instanceof TestURLClassLoader ? currentThreadClassLoader.getParent() : currentThreadClassLoader);
      
      // Replace the thread classloader - assumes
      // you have permissions to do so
      Thread.currentThread().setContextClassLoader(urlClassLoader);
    }
    
  }
  
  @Test
  public void testLoadingFromDirectory() throws Throwable {
    TestURLClassLoader.addToClasspath(Paths.get(getTestMethodInputDirectory() + "META-INF").toAbsolutePath().normalize().toUri().toURL());
    List<Path> paths = FileUtils.loadMetaInfFilesFromClassPath("testDir", "test.xml");
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("test.xml", paths.get(0).getFileName().toString());
    
    // make sure that I can read that path/file
    BufferedReader r = Files.newBufferedReader(paths.get(0));
    String s = r.readLine();
    Assert.assertEquals("<!-- This is a dummy file for testing purposes only. -->", s);
  }
  
  @Test
  public void testLoadingFromJar() throws Throwable {
    TestURLClassLoader.addToClasspath(URI.create("jar:file:"
                                                 + Paths.get(getTestMethodInputDirectory() + "test.jar").toAbsolutePath().normalize()
                                                 + "!/META-INF").toURL());
    List<Path> paths = FileUtils.loadMetaInfFilesFromClassPath("testDir**", "test.xml");
//    System.out.println(paths);
    Assert.assertEquals(1, paths.size());
    Assert.assertEquals("test.xml", paths.get(0).getFileName().toString());
    
    // make sure that I can read that path/file
    BufferedReader r = Files.newBufferedReader(paths.get(0));
    String s = r.readLine();
    Assert.assertEquals("<!-- This is a dummy file for testing purposes only. -->", s);
  }
  
}
