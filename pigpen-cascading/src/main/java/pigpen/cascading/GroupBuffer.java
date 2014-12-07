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
  private final List<Boolean> groupRequirements;

  public GroupBuffer(String init, String func, Fields fields, int numIterators, boolean groupAll, boolean joinNils, List<Boolean> groupRequirements) {
    super(fields);
    this.init = init;
    this.func = func;
    this.numIterators = numIterators;
    this.groupAll = groupAll;
    this.joinNils = joinNils;
    this.groupRequirements = groupRequirements;
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
    Object key = getKey(bufferCall.getGroup());
    List<Iterator> iterators = getIterators(bufferCall.getJoinerClosure(), bufferCall.getGroup());
    if (requiredGroupsPresent(iterators)) {
      Var emitFn = RT.var("pigpen.cascading.runtime", "emit-group-buffer-tuples");
      emitFn.invoke(fn, key, iterators, bufferCall.getOutputCollector(), groupAll);
    }
  }

  private Object getKey(TupleEntry group) {
    Object key = null;
    for (int i = 0; i < group.size(); i++) {
      if (key == null) {
        key = group.getObject(i);
      }
    }
    return key;
  }

  private boolean requiredGroupsPresent(List<Iterator> iterators) {
    boolean allPresent = true;
    for (int i = 0; i < groupRequirements.size(); i++) {
      if (groupRequirements.get(i) && !iterators.get(i).hasNext()) {
        allPresent = false;
        break;
      }
    }
    return allPresent;
  }

  private List<Iterator> getIterators(JoinerClosure joinerClosure, TupleEntry group) {
    List<Iterator> args = new ArrayList<Iterator>(numIterators);
    for (int i = 0; i < numIterators; i++) {
      boolean nullKey = group.getObject(i) == null;
      // Cascading doesn't provide the option of treating null keys on one side of the join
      // as the same but not on the other side. Thus we have to emulate that behavior by
      // pretending that one of the streams was not joined.
      Iterator<Tuple> iterator = (nullKey && !groupRequirements.get(i) && !joinNils) ? Collections.<Tuple>emptyList().iterator() : joinerClosure.getIterator(i);
      args.add(new BufferIterator(iterator));
    }
    return args;
  }
}
