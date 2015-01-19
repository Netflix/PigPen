package pigpen.cascading;

import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Var;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class PigPenFunction extends BaseOperation implements Function {

  private final String init;
  private final String func;

  public PigPenFunction(String init, String func, Fields fields) {
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
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    IFn fn = (IFn)functionCall.getContext();
    TupleEntry tupleEntry = functionCall.getArguments();
    System.out.println("tupleEntry = " + tupleEntry);
    Var emitFn = RT.var("pigpen.cascading.runtime", "emit-function-tuples");
    emitFn.invoke(fn, OperationUtil.deserialize(tupleEntry.getTuple()), functionCall.getOutputCollector());
  }
}
