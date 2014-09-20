package pigpen.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.IteratorSeq;
import org.apache.hadoop.io.BytesWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;

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

    // TODO: this should not use Tuple or any other Pig class.
    @Override
    public Object next() {
      Tuple t = delegate.next();
      Object obj = t.getObject(1);
      return TupleFactory.getInstance().newTuple(OperationUtil.getValue(obj));
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
    args.add(OperationUtil.getValue(group));
    args.add(wrapIterator(leftIterator));
    args.add(wrapIterator(rightIterator));
    Object result = fn.invoke(args);
    emitOutput(bufferCall, result);
  }

  // TODO: this should not use DataBag or any other Pig class.
  private DataBag wrapIterator(Iterator<Tuple> iterator) {
    return BagFactory.getInstance().newDefaultBag(IteratorSeq.create(new BufferIterator(iterator)));
  }

  // TODO: this should not use DataBag or any other Pig class.
  private void emitOutput(BufferCall call, Object result) {
    for (org.apache.pig.data.Tuple tuple : (DataBag)result) {
      try {
        Object[] objs = new Object[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
          Object obj = tuple.get(i);
          if (obj instanceof DataByteArray) {
            objs[i] = new BytesWritable(((DataByteArray) obj).get());
          } else {
            objs[i] = tuple.get(i);
          }
        }
        call.getOutputCollector().add(new Tuple(objs));
      } catch (ExecException e) {
        throw new RuntimeException();
      }
    }
  }
}
