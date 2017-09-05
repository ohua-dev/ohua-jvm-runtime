package ohua.runtime.engine.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import ohua.runtime.engine.exceptions.Assertion;

public abstract class FileUtils {
  public static class OhuaFileFilter implements FilenameFilter {
    private Pattern _filter = null;
    
    public OhuaFileFilter(String filter) {
      _filter = Pattern.compile(filter);
    }
    
    public boolean accept(File dir, String name) {
      boolean matches = _filter.matcher(name).find();
      return matches;
    }
  }
  
  public static void cleanupDirectory(File file) {
    if(!file.exists() || !file.isDirectory()) {
      return;
    }
    
    for(File chckPtFile : file.listFiles()) {
      if(chckPtFile.getName().startsWith(".")) {
        continue;
      }
      
      if(chckPtFile.isDirectory()) {
        cleanupDirectory(chckPtFile);
      }
      
      chckPtFile.delete();
    }
  }
  
  public static String loadFileContents(File file) throws IOException {
    FileReader reader = new FileReader(file);
    BufferedReader bReader = new BufferedReader(reader);
    StringBuilder builder = new StringBuilder();
    String line = bReader.readLine();
    while(line != null) {
      builder.append(line);
      line = bReader.readLine();
    }
    bReader.close();
    reader.close();
    return builder.toString();
  }
  
  public static final boolean isDirectoryEmpty(String path) {
    File dir = new File(path);
    assert dir.exists();
    assert dir.isDirectory();
    return dir.list().length < 1;
  }
  
  public static File[] loadFiles(File directory, String filter) {
    // get me the latest checkpoint
    return directory.listFiles(new OhuaFileFilter(filter));
  }
  
  public static void copy(String toCopy, String destination, String filter) throws FileNotFoundException {
    copy(new File(toCopy), new File(destination), filter);
  }
  
  public static void copy(File toCopy, File destination, String filter) throws FileNotFoundException {
    if(!toCopy.exists()) {
      throw new FileNotFoundException(toCopy.getAbsolutePath());
    }
    
    if(!destination.exists()) {
      throw new FileNotFoundException(destination.getAbsolutePath());
    }
    
    Assertion.invariant(destination.isDirectory());
    
    File copy = new File(destination.getAbsolutePath() + "/" + toCopy.getName());
    if(toCopy.isDirectory()) {
      if(!copy.exists()) {
        copy.mkdir();
      }
      
      for(File file : loadFiles(toCopy, filter)) {
        copy(file, copy, filter);
      }
    } else {
      copyFile(toCopy, copy);
    }
  }
  
  private static void copyFile(File toCopy, File copy) {
    try {
      boolean notAlreadyExists = copy.createNewFile();
      Assertion.invariant(notAlreadyExists);
      FileInputStream original = new FileInputStream(toCopy);
      FileOutputStream copyStream = new FileOutputStream(copy);
      byte[] buf = new byte[1024];
      int len = 0;
      while((len = original.read(buf)) > 0) {
        copyStream.write(buf, 0, len);
      }
      original.close();
      copyStream.close();
    }
    catch(IOException e) {
      Assertion.impossible(e);
    }
  }
  
  public static int getFileCountInDir(String filePath) {
    File dir = new File(filePath);
    assert dir.exists();
    assert dir.isDirectory();
    return dir.list().length;
  }
  
  public static void writeFile(File file, String contents) throws IOException {
    FileWriter writer = new FileWriter(file);
    writer.write(contents);
    writer.flush();
    writer.close();
  }
  
  public static List<Path> loadMetaInfFilesFromClassPath(String folder, String regex) {
    List<Path> result = new ArrayList<>();
    try {
      Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources("META-INF");
      while(resources.hasMoreElements()) {
        URL url = resources.nextElement();
        String[] fsAndFile = url.toURI().toString().split("!");
        URI uri = URI.create(fsAndFile[0]);
        if(fsAndFile.length == 2) {
          try {
            // don't close the file system! we might want to read from it and we can't once it
            // is closed!
            FileSystem fs = FileSystems.getFileSystem(uri);
            result.addAll(findURLs(fs, fsAndFile[1], folder, regex));
          }
          catch(FileSystemNotFoundException n) {
            // don't close the file system! we might want to read from it and we can't once it
            // is closed!
            FileSystem fs = FileSystems.newFileSystem(uri, new HashMap<>());
            result.addAll(findURLs(fs, fsAndFile[1], folder, regex));
          }
        } else {
          // don't close the file system! we might want to read from it and we can't once it is
          // closed! (can't close default fs anyways)
          FileSystem fs = FileSystems.getDefault();
          result.addAll(findURLs(fs, uri.getPath(), folder, regex));
        }
      }
    }
    catch(IOException | URISyntaxException ioe) {
      throw new RuntimeException(ioe);
    }
    return result;
  }
  
  private static List<Path>
      findURLs(FileSystem fs, String metaInfPath, String folder, String regex) throws IOException
  {
    Path metaInfDir = fs.getPath(metaInfPath);
    List<Path> found = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(metaInfDir, folder)) {
      for(Path entry : stream) {
        try (DirectoryStream<Path> fileStream = Files.newDirectoryStream(entry, regex)) {
          for(Path file : fileStream)
            found.add(file);
        }
      }
    }
    return found;
  }
  
  // TODO refactoring: findURLs should be a "partial" function. (Clojure is such a nice language
  // ... sigh)
  public static List<Path> loadFromClasspath(String folder, String regex) {
    List<Path> result = new ArrayList<>();
    try {
      Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(folder);
      while(resources.hasMoreElements()) {
        URL url = resources.nextElement();
        String[] fsAndFile = url.toURI().toString().split("!");
        URI uri = URI.create(fsAndFile[0]);
        if(fsAndFile.length == 2) {
          try {
            // don't close the file system! we might want to read from it and we can't once it
            // is closed!
            FileSystem fs = FileSystems.getFileSystem(uri);
            result.addAll(findURLs(fs, fsAndFile[1], regex));
          }
          catch(FileSystemNotFoundException n) {
            // don't close the file system! we might want to read from it and we can't once it
            // is closed!
            FileSystem fs = FileSystems.newFileSystem(uri, new HashMap<>());
            result.addAll(findURLs(fs, fsAndFile[1], regex));
          }
        } else {
          // don't close the file system! we might want to read from it and we can't once it
          // is closed! (can't close default fs anyways)
          FileSystem fs = FileSystems.getDefault();
          result.addAll(findURLs(fs, uri.getPath(), regex));
        }
      }
    }
    catch(IOException | URISyntaxException ioe) {
      throw new RuntimeException(ioe);
    }
    return result;
  }
  
  private static List<Path> findURLs(FileSystem fs, String folder, String regex) throws IOException {
    Path folderPath = fs.getPath(folder);
    List<Path> found = new ArrayList<>();
    try (DirectoryStream<Path> fileStream = Files.newDirectoryStream(folderPath, regex)) {
      for(Path file : fileStream)
        found.add(file);
    }
    return found;
  }
  
}
