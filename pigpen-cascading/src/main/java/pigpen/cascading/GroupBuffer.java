package pigpen.cascading;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class GroupBuffer extends BaseOperation implements Buffer {

  private static class BufferIterator implements Iterator {

    private final Iterator<Tuple> delegate;

    private BufferIterator(Iterator<Tuple> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Object next() {
      Tuple t = delegate.next();
      return t.getObject(1);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private final String init;
  private final String func;
  private final int numIterators;
  private final boolean groupAll;
  private final boolean joinNils;
  private final boolean isFullOuter;
  private final List<Boolean> groupRequirements;

  public GroupBuffer(String init, String func, Fields fields, int numIterators, boolean groupAll, boolean joinNils, List<Boolean> groupRequirements) {
    super(fields);
    this.init = init;
    this.func = func;
    this.numIterators = numIterators;
    this.groupAll = groupAll;
    this.joinNils = joinNils;
    this.groupRequirements = groupRequirements;
    boolean fullOuter = true;
    for (Boolean isRequired : groupRequirements) {
      if (isRequired) {
        fullOuter = false;
      }
    }
    this.isFullOuter = fullOuter;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    super.prepare(flowProcess, operationCall);
    OperationUtil.init(init);
    IFn fn = OperationUtil.getFn(func);
    operationCall.setContext(fn);
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
    IFn fn = (IFn)bufferCall.getContext();
    boolean allKeysNull = allKeysNull(bufferCall.getGroup());
    List<Object> keys = getKeys(bufferCall.getGroup(), allKeysNull);
    List<List<Iterator>> iteratorsList = getIterators(bufferCall.getJoinerClosure(), bufferCall.getGroup(), allKeysNull);
    for (int i = 0; i < iteratorsList.size(); i++) {
      List<Iterator> iterators = iteratorsList.get(i);
      Object key = keys.get(i);
      if (requiredGroupsPresent(iterators)) {
        Var emitFn = RT.var("pigpen.cascading.runtime", "emit-group-buffer-tuples");
        emitFn.invoke(fn, key, iterators, bufferCall.getOutputCollector(), groupAll);
      }
    }
  }

  private boolean allKeysNull(TupleEntry group) {
    for (int i = 0; i < group.size(); i++) {
      if (group.getObject(i) != null) {
        return false;
      }
    }
    return true;
  }

  private List<Object> getKeys(TupleEntry group, boolean allKeysNull) {
    List<Object> keys = new ArrayList<Object>();
    if (!isFullOuter || joinNils || !allKeysNull) {
      Object key = null;
      for (int i = 0; i < group.size(); i++) {
        if (key == null) {
          key = group.getObject(i);
        }
      }
      keys.add(key);
    } else {
      for (int i = 0; i < numIterators; i++) {
        keys.add(group.getObject(i));
      }
    }
    return keys;
  }

  private boolean requiredGroupsPresent(List<Iterator> iterators) {
    for (int i = 0; i < groupRequirements.size(); i++) {
      if (groupRequirements.get(i) && !iterators.get(i).hasNext()) {
        return false;
      }
    }
    return true;
  }

  private List<List<Iterator>> getIterators(JoinerClosure joinerClosure, TupleEntry group, boolean allKeysNull) {
    List<List<Iterator>> ret = new ArrayList<List<Iterator>>();
    if (!isFullOuter || joinNils || !allKeysNull) {
      List<Iterator> args = new ArrayList<Iterator>(numIterators);
      for (int i = 0; i < numIterators; i++) {
        boolean nullKey = group.getObject(i) == null;
        // Cascading doesn't provide the option of treating null keys on one side of the join
        // as the same but not on the other side. Thus we have to emulate that behavior by
        // pretending that one of the streams was not joined.
        Iterator<Tuple> iterator = (nullKey && !groupRequirements.get(i) && !joinNils) ? Collections.<Tuple>emptyList().iterator() : joinerClosure.getIterator(i);
        args.add(new BufferIterator(iterator));
      }
      ret.add(args);
    } else {
      // Emulate joinNils == false by pretending the streams don't belong to the same group.
      for (int active = 0; active < numIterators; active++) {
        List<Iterator> args = new ArrayList<Iterator>(numIterators);
        for (int i = 0; i < numIterators; i++) {
          Iterator<Tuple> iterator = (i == active) ? joinerClosure.getIterator(i) : Collections.<Tuple>emptyList().iterator();
          args.add(new BufferIterator(iterator));
        }
        ret.add(args);
      }
    }
    return ret;
  }
}
