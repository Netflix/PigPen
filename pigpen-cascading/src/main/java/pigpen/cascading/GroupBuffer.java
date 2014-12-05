package pigpen.cascading;

import java.util.ArrayList;
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

  public GroupBuffer(String init, String func, Fields fields, int numIterators, boolean groupAll) {
    super(fields);
    this.init = init;
    this.func = func;
    this.numIterators = numIterators;
    this.groupAll = groupAll;
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
    Object group = bufferCall.getGroup().getObject(0);
    JoinerClosure joinerClosure = bufferCall.getJoinerClosure();
    Var emitFn = RT.var("pigpen.cascading.runtime", "emit-group-buffer-tuples");
    emitFn.invoke(fn, group, getIterators(joinerClosure), bufferCall.getOutputCollector(), groupAll);
  }

  private List<Iterator> getIterators(JoinerClosure joinerClosure) {
    List<Iterator> args = new ArrayList<Iterator>(numIterators);
    for (int i = 0; i < numIterators; i++) {
      Iterator<Tuple> iterator = joinerClosure.getIterator(i);
      args.add(new BufferIterator(iterator));
    }
    return args;
  }
}
