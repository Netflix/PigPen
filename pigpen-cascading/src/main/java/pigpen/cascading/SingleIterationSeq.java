package pigpen.cascading;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.Iterator;

import clojure.lang.ASeq;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;

/**
 * This sequence behaves like a normal iterator-seq up to a certain size.
 * If the max size is exceeded, it will only allow one iteration. The objective
 * is to become immune to memory problems resulting from holding onto the head for
 * large collections but still support operations such as count, max, etc.
 */
public class SingleIterationSeq extends ASeq {
  private static int MAX_SIZE = 1000;

  private final Iterator iter;
  private final State state;

  private static class State {
    volatile Object val;
    volatile Object rest;
    volatile int index;
  }

  public static SingleIterationSeq create(Iterator iter) {
    return create(iter, 0);
  }

  private static SingleIterationSeq create(Iterator iter, int index) {
    if (iter.hasNext()) {
      return new SingleIterationSeq(iter, index);
    }
    return null;
  }

  private SingleIterationSeq(Iterator iter, int index) {
    this.iter = iter;
    state = new State();
    this.state.val = state;
    this.state.rest = state;
    this.state.index = index;
  }

  private SingleIterationSeq(IPersistentMap meta, Iterator iter, State state) {
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
    if (state.rest == state) {
      synchronized (state) {
        if (state.rest == state) {
          first();
          if (state.index < MAX_SIZE) {
            state.rest = create(iter, state.index + 1);
            return (ISeq)state.rest;
          } else {
            state.rest = iter;
            return create(iter, state.index + 1);
          }
        }
      }
    }
    if (state.rest == iter) {
      throw new UnsupportedOperationException("This Seq can only be traversed once.");
    }
    return (ISeq)state.rest;
  }

  public SingleIterationSeq withMeta(IPersistentMap meta) {
    return new SingleIterationSeq(meta, iter, state);
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    throw new NotSerializableException(getClass().getName());
  }
}
