package pigpen.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class PigPenBuffer extends BaseOperation implements Buffer {

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

  public PigPenBuffer(String init, String func, Fields fields) {
    super(fields);
    this.init = init;
    this.func = func;
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
    // TODO: there may be more than 2 streams. This shouldn't be hardcoded.
    Iterator<Tuple> leftIterator = joinerClosure.getIterator(0);
    Iterator<Tuple> rightIterator = joinerClosure.getIterator(1);
    List args = new ArrayList(3);
    args.add(group);
    args.add(wrapIterator(leftIterator));
    args.add(wrapIterator(rightIterator));
    Object result = fn.invoke(args);
    //    emitOutput(bufferCall, result);
  }

  private ISeq wrapIterator(Iterator iterator) {
    return IteratorSeq.create(new BufferIterator(iterator));
  }
}
