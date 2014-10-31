package pigpen.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import clojure.lang.LazySeq;
import clojure.lang.PersistentVector;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.pipe.joiner.JoinerClosure;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

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

  public GroupBuffer(String init, String func, Fields fields, int numIterators) {
    super(fields);
    this.init = init;
    this.func = func;
    this.numIterators = numIterators;
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
    List args = new ArrayList(3);
    args.add(group);
    for (int i = 0; i < numIterators; i++) {
      Iterator<Tuple> iterator = joinerClosure.getIterator(i);
      args.add(wrapIterator(iterator));
    }
    // TODO: do this in clojure
    LazySeq result = (LazySeq)fn.invoke(args);
    OperationUtil.emitOutputTuples(bufferCall.getOutputCollector(), result);
  }

  private ISeq wrapIterator(Iterator iterator) {
    return IteratorSeq.create(new BufferIterator(iterator));
  }
}
