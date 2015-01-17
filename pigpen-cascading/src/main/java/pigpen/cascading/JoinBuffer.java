package pigpen.cascading;

import java.util.Iterator;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class JoinBuffer extends BaseOperation implements Buffer {

  private static class BufferIterator implements Iterator {

    private final Iterator<TupleEntry> delegate;

    private BufferIterator(Iterator<TupleEntry> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public Object next() {
      TupleEntry entry = delegate.next();
      return OperationUtil.deserialize(entry.getTuple());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private final String init;
  private final String func;
  private final boolean allArgs;

  public JoinBuffer(String init, String func, Fields fields, boolean allArgs) {
    super(fields);
    this.init = init;
    this.func = func;
    this.allArgs = allArgs;
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
    Iterator<TupleEntry> iterator = new BufferIterator(bufferCall.getArgumentsIterator());
    Var emitFn = RT.var("pigpen.cascading.runtime", "emit-join-buffer-tuples");
    emitFn.invoke(fn, iterator, bufferCall.getOutputCollector(), allArgs);
  }
}
