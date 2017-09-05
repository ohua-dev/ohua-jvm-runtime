/*
 * Copyright (c) Sebastian Ertel 2008. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.sections;

import ohua.runtime.engine.flowgraph.elements.operator.Arc;

@Deprecated // remove and just provide a service inside the section graph that takes an operator and returns the current section it is located on.
public class SectionArc //implements GraphArc<AbstractSection>
{
  private Arc _mappedArc = null;
  
  private AbstractSection _sourceSection = null;
  
  private AbstractSection _targetSection = null;

  public Arc getMappedArc()
  {
    return _mappedArc;
  }

  public void setMappedArc(Arc mappedArc)
  {
    _mappedArc = mappedArc;
  }

  public AbstractSection getSourceSection()
  {
    return _sourceSection;
  }

  public void setSourceSection(AbstractSection sourceSection)
  {
    _sourceSection = sourceSection;
  }

  public AbstractSection getTargetSection()
  {
    return _targetSection;
  }

  public void setTargetSection(AbstractSection targetSection)
  {
    _targetSection = targetSection;
  }

  public AbstractSection getSource()
  {
    return _sourceSection;
  }

  public AbstractSection getTarget()
  {
    return _targetSection;
  }
}
