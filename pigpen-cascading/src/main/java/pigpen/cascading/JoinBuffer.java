package pigpen.cascading;

import java.util.Arrays;
import java.util.Iterator;

import clojure.lang.IFn;
import clojure.lang.LazySeq;
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

  private final String init;
  private final String func;

  public JoinBuffer(String init, String func, Fields fields) {
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
    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
    Var emitFn = RT.var("pigpen.cascading.runtime", "emit-join-buffer-tuples");
    emitFn.invoke(fn, iterator, bufferCall.getOutputCollector());
  }
}
