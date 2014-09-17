package pigpen.cascading;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PigPenFunction extends BaseOperation implements Function {

  private static final IFn EVAL_STRING = RT.var("pigpen.pig", "eval-string");
  private final String init;
  private final String func;

  public PigPenFunction(String init, String func) {
    super(new Fields());
    EVAL_STRING.invoke(init);
    this.init = init;
    this.func = func;
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    super.prepare(flowProcess, operationCall);
    EVAL_STRING.invoke(init);
    IFn fn = (IFn)EVAL_STRING.invoke(func);
    operationCall.setContext(fn);
  }

  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    TupleEntry tupleEntry = functionCall.getArguments();
    IFn fn = (IFn)functionCall.getContext();
    Object result = fn.invoke(tupleEntry.getTuple());
    emitOutput(functionCall, result);
  }

  // TODO: this should not use DataBag or any other Pig class.
  private void emitOutput(FunctionCall functionCall, Object result) {
    for (org.apache.pig.data.Tuple tuple : (DataBag)result) {
      try {
        Object[] objs = new Object[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
          objs[i] = tuple.get(i);
        }
        functionCall.getOutputCollector().add(new Tuple(objs));
      } catch (ExecException e) {
        throw new RuntimeException();
      }
    }
  }
}
