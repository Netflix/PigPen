package pigpen.cascading;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.Iterator;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;

/**
 * Sequence that allows only one iteration, but is immune to
 * memory problems resulting from holding onto the head.
 */
public class SingleIterationSeq extends ASeq {
  final Iterator iter;
  final State state;

  static class State {
    volatile Object val;
    volatile boolean isRealized;
  }

  public static SingleIterationSeq create(Iterator iter) {
    if (iter.hasNext()) {
      return new SingleIterationSeq(iter);
    }
    return null;
  }

  SingleIterationSeq(Iterator iter) {
    this.iter = iter;
    state = new State();
    this.state.val = state;
    this.state.isRealized = false;
  }

  SingleIterationSeq(IPersistentMap meta, Iterator iter, State state) {
    super(meta);
    this.iter = iter;
    this.state = state;
  }

  public Object first() {
    if (state.val == state) {
      synchronized (state) {
        if (state.val == state) {
          state.val = iter.next();
        }
      }
    }
    return state.val;
  }

  public ISeq next() {
    if (state.isRealized == false) {
      synchronized (state) {
        if (state.isRealized == false) {
          first();
          state.isRealized = true;
          return create(iter);
        }
      }
    }
    throw new UnsupportedOperationException("This Seq can only be traversed once.");
  }

  public SingleIterationSeq withMeta(IPersistentMap meta) {
    return new SingleIterationSeq(meta, iter, state);
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    throw new NotSerializableException(getClass().getName());
  }
}
