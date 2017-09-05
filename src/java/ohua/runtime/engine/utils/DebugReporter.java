/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.logging.Logger;

import ohua.runtime.engine.exceptions.InvariantBroken;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.sections.Section;

@Deprecated
public class DebugReporter
{
  public interface BugReport
  {
    public void printBugReport();
  }
  
  public static class SectionBugReport implements BugReport
  {
    private Section _section = null;
    
    public SectionBugReport(Section section)
    {
      _section = section;
    }
    
    public void printBugReport()
    {
      DebugReporter.getInstance().raiseInvariantBroken(_section);
    }
    
  }

  private static DebugReporter _singleton = new DebugReporter();
  private Logger _logger = OhuaLoggerFactory.getLogger(getClass());
  
  private DebugReporter()
  {
    // singleton
  }
  
  public static DebugReporter getInstance()
  {
    return _singleton;
  }

  public void raiseInvariantBroken(Collection<Section> sections)
  {
    InvariantBroken invariantBroken = new InvariantBroken();
    for(Section section : sections)
    {
      section.printSectionInfo(_logger);
      _logger.info("Activated operators:");
      for(OperatorCore operator : section.getOperators())
      {
//        if(operator.isActive())
//        {
//          _logger.info(operator.getID());
//          _logger.info("operator state: " + operator.getOperatorState());
//        }
      }
    }
    
    StringWriter writer = new StringWriter();
    invariantBroken.printStackTrace(new PrintWriter(writer));
    _logger.warning(writer.toString());
  }
  
  public void raiseInvariantBroken(Section section)
  {
    InvariantBroken invariantBroken = new InvariantBroken();
    section.printSectionInfo(_logger);
    _logger.info("Contained operators:");
    for(OperatorCore operator : section.getOperators())
    {
        _logger.info(operator.getID());
//        _logger.info("operator state: " + operator.getOperatorState());
    }
    
    _logger.info("Activated operators:");
    for(OperatorCore operator : section.getOperators())
    {
//      if(operator.isActive())
//      {
//        _logger.info(operator.getID());
//      }
    }
    
//    _logger.info("Operators in scheduling queue:" + section.getActiveOperators());

    StringWriter writer = new StringWriter();
    invariantBroken.printStackTrace(new PrintWriter(writer));
    _logger.warning(writer.toString());
  }

  public void printOperatorReport(OperatorCore operator)
  {
    _logger.warning("Operator " + operator.getID()
                    + " has encountered a problem and is throwing an exception!");
//    _logger.warning("operator state: " + operator.getOperatorState());
    _logger.warning("graph level: " + operator.getLevel());
//    _logger.warning("scheduling priority: " + operator.getSchedulingPriority());
//    _logger.warning("system phase: " + operator.getProcessState());
  }

}
