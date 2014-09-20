package pigpen.cascading;

import java.util.Iterator;

import clojure.lang.IFn;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class PigPenBuffer extends BaseOperation implements Buffer {

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
    System.out.println("group = " + group);
    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
    while (iterator.hasNext()) {
      TupleEntry te = iterator.next();
      System.out.println("te = " + te);
    }
  }
}
