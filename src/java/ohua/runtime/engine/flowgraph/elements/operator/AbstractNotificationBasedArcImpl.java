/*
 * Copyright (c) Sebastian Ertel 2017. All Rights Reserved.
 *
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */
package ohua.runtime.engine.flowgraph.elements.operator;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Created by sertel on 1/28/17.
 */
public abstract class AbstractNotificationBasedArcImpl extends AbstractArcImpl {
  private boolean _downStreamNotificationEnabled = true;
  private boolean _upStreamNotificationEnabled = true;

  private BiConsumer<Arc, Arc> _activateUpstreamDefault;// = (k, l) -> k.getTarget().addUpstreamOpToBeActivated(l.getTargetPort());
  private BiConsumer<Arc, Arc> _activateDownstreamDefault;// = (k,l) -> k.getSource().addDownstreamOpToBeActivated(l);

  private BiConsumer<Arc, Arc> _activateUpstream = _activateUpstreamDefault;
  private BiConsumer<Arc, Arc> _activateDownstream = _activateDownstreamDefault;

  protected Arc _arc;
  private Set<ArcListener> _sourceListeners = new HashSet<>();
  private Set<ArcListener> _targetListeners = new HashSet<>();

  public AbstractNotificationBasedArcImpl(Arc arc) {
    _arc = arc;
  }

  public void setDefaultDownstreamActivation(BiConsumer<Arc, Arc> activateDownstreamDefault){
    _activateDownstreamDefault = activateDownstreamDefault;
    _activateDownstream = _activateDownstreamDefault;
  }

  public void setDefaultUpstreamActivation(BiConsumer<Arc, Arc> activateUpstreamDefault){
    _activateUpstreamDefault = activateUpstreamDefault;
    _activateUpstream = _activateUpstreamDefault;
  }

  public void enableDownstreamNotification() {
    _downStreamNotificationEnabled = true;
  }

  public void disableDownstreamActivation() { _downStreamNotificationEnabled = false; }

  public void enableUpstreamNotification() {
    _upStreamNotificationEnabled = true;
  }

  public void disableUpstreamActivation() {
    _upStreamNotificationEnabled = false;
  }

  public boolean isBlocking() {
    return false;
  }

  protected void activateUpstream() {
    if(_upStreamNotificationEnabled) {
      if(!_arc.getTargetPort().isMetaPort()) {
        _activateUpstream.accept(_arc, _arc);
      }
    }
  }

  protected void activateUpstream(Arc arc) {
    if(_upStreamNotificationEnabled) {
      if(!_arc.getTargetPort().isMetaPort()) {
        _activateUpstream.accept(arc, _arc);
      }
    }
  }

  protected void activateUpstreamDefault(Arc arc) {
    if(_upStreamNotificationEnabled) {
      if(!_arc.getTargetPort().isMetaPort()) {
        _activateUpstreamDefault.accept(arc, _arc);
      }
    }
  }

  protected void activateDownstream() {
    if(_downStreamNotificationEnabled)
      _activateDownstream.accept(_arc, _arc);
  }

  protected void activateDownstream(Arc arc) {
    if(_downStreamNotificationEnabled)
      _activateDownstream.accept(arc, _arc);
  }

  protected void activateDownstreamDefault(Arc arc) {
    if(_downStreamNotificationEnabled)
      _activateDownstreamDefault.accept(arc, _arc);
  }

  public final void setDownstreamActivation(BiConsumer<Arc, Arc> activateDownstream){
    _activateDownstream = activateDownstream;
  }

  public final void setUpstreamActivation(BiConsumer<Arc, Arc> activateUpstream){
    _activateUpstream = activateUpstream;
  }

  public void registerArcEventListener(ArcListener arcListener) {
    _sourceListeners.add(arcListener);
    _targetListeners.add(arcListener);
  }

  public void unregisterArcEventListener(ArcListener arcListener) {
    _sourceListeners.remove(arcListener);
    _targetListeners.remove(arcListener);
  }

  public void unregisterArcEventListener(ArcListener arcListener, AbstractNotificationBasedArcImpl.ArcEvent event) {
    switch (event) {
      case DATA_NEEDED:
        _sourceListeners.remove(arcListener);
        break;
      case DATA_AVAILABLE:
      case DEQUEUE_NEEDED:
        _targetListeners.remove(arcListener);
        break;
    }
  }

  public void notifyDataNeeded() {
    if (!_sourceListeners.isEmpty()) {
      for (ArcListener listener : _sourceListeners) {
        listener.notifyOnArcEvent(AbstractNotificationBasedArcImpl.ArcEvent.DATA_NEEDED, _arc);
      }
    }
  }

  public void notifyDequeueNeeded() {
    if (!_targetListeners.isEmpty()) {
      for (ArcListener listener : _targetListeners) {
        listener.notifyOnArcEvent(AbstractNotificationBasedArcImpl.ArcEvent.DEQUEUE_NEEDED, _arc);
      }
    }
  }

  public void notifyDataAvailable() {
    if (!_targetListeners.isEmpty()) {
      for (ArcListener listener : _targetListeners) {
        listener.notifyOnArcEvent(AbstractNotificationBasedArcImpl.ArcEvent.DATA_AVAILABLE, _arc);
      }
    }
  }

  public Set<ArcListener> getTargetListeners() {
    return _targetListeners;
  }

  public Set<ArcListener> getSourceListeners() {
    return _sourceListeners;
  }

  public enum ArcEvent {
    DATA_NEEDED,
    DEQUEUE_NEEDED,
    DATA_AVAILABLE
  }

  public interface ArcListener {
    void notifyOnArcEvent(ArcEvent event, Arc arc);
  }
}
