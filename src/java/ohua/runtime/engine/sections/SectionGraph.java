/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import ohua.runtime.engine.exceptions.Assertion;
import ohua.runtime.engine.flowgraph.elements.operator.Arc;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorCore;
import ohua.runtime.engine.flowgraph.elements.operator.OperatorID;

public class SectionGraph
{

  class UserGraph
  {
    List<Section> _inputSections = new ArrayList<>();
    List<Section> _outputSections = new ArrayList<>();
//    List<Section> _ioSections = new ArrayList<>();
    List<Section> _computationalSections = new ArrayList<>();
  }
  
  class SystemGraph
  {
    UserGraph _userGraph = new UserGraph();
    
    MetaSection _userGraphEntranceSection = null;
    MetaSection _userGraphExitSection = null;
    
    List<MetaSection> _systemSections = new ArrayList<>();

    public List<MetaSection> getContainedGraphNodes()
    {
      List<MetaSection> graphNodes = new ArrayList<>();
      graphNodes.addAll(_systemSections);
      graphNodes.add(_userGraphEntranceSection);
      graphNodes.add(_userGraphExitSection);
      return graphNodes;
    }
  }
  
  private SystemGraph _graph = new SystemGraph();
  
  private Map<OperatorID, Section> _operatorSectionMap = new HashMap<>();
  private Map<String, OperatorCore> _operatorRegistry = new HashMap<>();
  private Map<OperatorID, OperatorCore> _operatorIDRegistry = new HashMap<>();
  
  private ActivationService _service = null;
  
  public List<Section> getComputationalSections()
  {
    return Collections.unmodifiableList(_graph._userGraph._computationalSections);
  }
  
  public void setComputationalSections(List<Section> computationalSections)
  {
    _graph._userGraph._computationalSections = computationalSections;
    updateOperatorRegistry(computationalSections);
  }
  
  public List<Arc> getInterSectionArcs()
  {
    return _graph._userGraph._computationalSections.stream()
            .flatMap(s -> s.getOutgoingArcs().stream())
            .filter(a -> !a.getTargetPort().isMetaPort())
            .collect(Collectors.toList());
  }

  public List<Arc> getSystemInterSectionArcs()
  {
    List<Arc> arcs = _graph.getContainedGraphNodes().stream().flatMap(s -> s.getOutgoingArcs().stream()).collect(Collectors.toList());
    arcs.addAll(_graph._userGraphExitSection.getIncomingArcs());
    return arcs;
  }

//  public List<Section> getIoSections()
//  {
////    return Collections.unmodifiableList(_graph._userGraph._ioSections);
////  }
//
//  public void setIoSections(List<Section> ioSections)
//  {
//    _graph._userGraph._ioSections = ioSections;
//  }
  
  public Set<Section> getAllSections()
  {
    Set<Section> all = new HashSet<>();
//    all.addAll(_graph._userGraph._ioSections);
    // it is possible for input and output sections to not perform any I/O
    all.addAll(_graph._userGraph._outputSections);
    all.addAll(_graph._userGraph._inputSections);
    all.addAll(_graph._userGraph._computationalSections);
    return all;
  }

  public Set<Section> getEntireSectionWorld()
  {
    Set<Section> all = getAllSections();
    all.addAll(_graph._systemSections);
    all.add(_graph._userGraphEntranceSection);
    all.add(_graph._userGraphExitSection);
    return all;
  }

  public List<Section> getContainedGraphNodes()
  {
    List<Section> nodes = new ArrayList<Section>();
    nodes.addAll(getAllSections());
    return nodes;
  }
  
  public List<Section> getInputSections()
  {
    return Collections.unmodifiableList(_graph._userGraph._inputSections);
  }
  
  public void setInputSections(List<Section> inputSections)
  {
    _graph._userGraph._inputSections = inputSections;
    updateOperatorRegistry(inputSections);
  }

  public List<Section> getOutputSections()
  {
    return Collections.unmodifiableList(_graph._userGraph._outputSections);
  }

  public void setOutputSections(List<Section> outputSections)
  {
    _graph._userGraph._outputSections = outputSections;
    updateOperatorRegistry(outputSections);
  }
  
  public void updateOperatorRegistry(List<Section> newSections)
  {
    for(Section newSection : newSections)
    {
      for(OperatorCore operator : newSection.getOperators())
      {
        Assertion.invariant(operator.getOperatorName() != null);
        _operatorRegistry.put(operator.getOperatorName(), operator);
        _operatorIDRegistry.put(operator.getId(), operator);
        _operatorSectionMap.put(operator.getId(), newSection);
      }
    }
  }
  
  public Section findParentSection(OperatorID operator)
  {
    return _operatorSectionMap.get(operator);
  }
  
  public Set<OperatorCore> getAllOperators()
  {
    Set<OperatorCore> allOps = new HashSet<OperatorCore>();
    for(Section section : getAllSections())
    {
      allOps.addAll(section.getOperators());
    }
    
    return allOps;
  }
  
  public void debug()
  {
    Logger logger = Logger.getLogger(getClass().getCanonicalName());
    logger.log(Level.FINE, "# input sections: " + _graph._userGraph._inputSections.size());
    logger.log(Level.FINE, "# computational sections: "
                           + _graph._userGraph._computationalSections.size());
    logger.log(Level.FINE, "# output sections: " + _graph._userGraph._outputSections.size());
    
    logger.log(Level.FINE, "Input Section info:");
    for(AbstractSection inputSection : _graph._userGraph._inputSections)
    {
      inputSection.printSectionInfo();
    }
    
    logger.log(Level.FINE, "Output Section info:");
    for(AbstractSection outputSection : _graph._userGraph._outputSections)
    {
      outputSection.printSectionInfo();
    }
    
    logger.log(Level.FINE, "Computational Section info:");
    for(AbstractSection computationalSection : _graph._userGraph._computationalSections)
    {
      computationalSection.printSectionInfo();
    }
  }
  
  public void setUserGraphExitSection(MetaSection userGraphExitSection)
  {
    _graph._userGraphExitSection = userGraphExitSection;
    updateOperatorRegistry(Collections.singletonList((Section) userGraphExitSection));
  }
  
  public MetaSection getUserGraphExitSection()
  {
    return _graph._userGraphExitSection;
  }
  
  public void setUserGraphEntranceSection(MetaSection metaSourceSection)
  {
    _graph._userGraphEntranceSection = metaSourceSection;
    updateOperatorRegistry(Collections.singletonList((Section) metaSourceSection));
  }
  
  public MetaSection getUserGraphEntranceSection()
  {
    return _graph._userGraphEntranceSection;
  }
  
  public void addSystemSection(MetaSection systemSection)
  {
    _graph._systemSections.add(systemSection);
    updateOperatorRegistry(Collections.singletonList((Section) systemSection));
  }

  public List<MetaSection> getSystemSections()
  {
    return Collections.unmodifiableList(_graph._systemSections);
  }

  public SystemGraphNodeIterator getSystemGraphIterator()
  {
    return new SystemGraphNodeIterator(this);
  }
  
  public OperatorCore findOperator(String operatorName)
  {
    return _operatorRegistry.get(operatorName);
  }
  
  public OperatorCore findOperator(OperatorID operatorID)
  {
    return _operatorIDRegistry.get(operatorID);
  }
  
  public List<Arc> getAllArcs()
  {
    List<Arc> arcs = new ArrayList<>();
    for(OperatorCore operator : getEntireOperatorWorld())
    {
      arcs.addAll(operator.getGraphNodeInputConnections());
    }
    return arcs;
  }
  
//  /**
//   * Currently only input sections can be periodic.
//   * @return
//   */
//  public boolean hasPeriodicUserSections()
//  {
//    for(AbstractSection section : _graph._userGraph._inputSections)
//    {
//      if(section.isPeriodic())
//      {
//        return true;
//      }
//    }
//
//    return false;
//  }

//  // FIXME remove from section graph!
//  public void registerActivationService(ActivationService service)
//  {
//    _service = service;
//  }
//  // FIXME remove from section graph!
//  public ActivationService getActivationService()
//  {
//    return _service;
//  }
  
  public void remove(OperatorCore orphan)
  {
    Section parent = findParentSection(orphan.getId());
    if(parent.getOperators().size() == 1)
    {
      _graph._userGraph._computationalSections.remove(parent);
      _graph._userGraph._inputSections.remove(parent);
      _graph._userGraph._outputSections.remove(parent);
    }
    _operatorIDRegistry.remove(orphan.getId());
    _operatorSectionMap.remove(orphan.getId());
    _operatorRegistry.remove(orphan.getOperatorName());
  }

  public Set<OperatorCore> getEntireOperatorWorld()
  {
    Set<OperatorCore> userOps = getAllOperators();
    userOps.add(_graph._userGraphEntranceSection.getOperator());
    userOps.add(_graph._userGraphExitSection.getOperator());
    for(MetaSection sysSec : _graph._systemSections)
    {
      userOps.add(sysSec.getOperator());
    }
    return userOps;
  }

  public boolean isSourceOperator(OperatorID op)
  {
    return !findOperator(op).isSystemInputOperator();
  }
  
}
