/*
 * Copyright � Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.daapi;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Path examples:<br>
 * element1/element2/vector[2]/element3 -> returns a single value<br>
 * element1/element2/vector/element3 -> returns all values under the vector<br>
 * 
 * @author sertel
 * 
 */
public interface OhuaDataAccessor
{
  /**
   * The accessor will resolve the paths starting with this object. That will allow us to
   * support even relative paths.
   * @param resolutionRoot
   */
  public void setResolutionRoot(String pathToResolutionRoot);
  
  /**
   * The accessor will resolve the paths starting with this object. That will allow us to
   * support even relative paths.
   * @param resolutionRoot
   */
  public void setDataRoot(Object resolutionRoot);
  
  /**
   * The accessor is responsible for collecting and returning all data of an inner vector. Hence
   * it is supposed to know how to iterate over inner vectors.
   * @param path
   * @return
   */
  public List<Object> getData(String path);
  
  /**
   * A value to be added to the item pointed to by the path.
   * @param path
   * @param value
   */
  public void setData(String path, Object value);
  
  /**
   * The idea: One data format has to say from which other data formats it can convert from or
   * to. For example JSON can convert easily to and from XML (text). Hence when we define a
   * schema it should be possible for data format to also load a schema from another format.<br>
   * All formats will declare a specific suffix for the files that can contain schemas. Source
   * operators will never get fully qualified filenames. They will only have the name of the
   * file and the data format will be derived from the RuntimeProcessConfiguration according to
   * the selected data model. That way the process description and the operator will be
   * completely independent of the data format.
   */
  public void load(File file);
  
  public Set<String> getLeafs();
  
  public String dataToString(String format);
  
  /**
   * This function should provide the counterpart to the "serialization" function above.
   * @param data
   */
  public void parse(String data, String format);
  
  public Object getDataRoot();
  
  /**
   * This function can be used in order to understand whether an element in the tree exists. The
   * setData() and getData() will not be that graceful and throw an exception when the
   * referenced item does not exist.
   * @param string
   * @return
   */
  public boolean elementExists(String string);
}
