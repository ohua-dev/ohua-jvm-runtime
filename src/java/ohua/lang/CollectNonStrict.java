package ohua.lang;

import ohua.runtime.lang.operator.DataflowFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sertel on 9/9/16.
 */
public class CollectNonStrict {

    private List<Object> _items = null;

  @defsfn
  @DataflowFunction
  public Continuations collect(int size, NonStrict<?> item) {
    if (size > 0) {
      if (_items == null) _items = new ArrayList<>(size);
      return Continuations.empty()
              .at(item,
                      v -> {
                        _items.add(v);
                        if (_items.size() == size) {
                          List values = _items;
                          _items = null;
                          return Continuations.finish(values);
                        } else {
                          return Continuations.finish(DataflowFunction.Control.DROP);
                        }
                      });
    } else { // empty collections
      return Continuations.empty().handsOff(item);
    }
  }
}
