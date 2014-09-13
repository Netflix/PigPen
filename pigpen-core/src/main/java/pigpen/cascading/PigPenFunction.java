package pigpen.cascading;

import java.util.ArrayList;
import java.util.List;

import clojure.lang.IFn;
import clojure.lang.RT;
import org.apache.pig.data.DataBag;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PigPenFunction extends BaseOperation implements Function {

  private static final IFn EVAL_STRING = RT.var("pigpen.pig", "eval-string");
  private final String init;
  private final String func;

  public PigPenFunction(String init, String func) {
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
    DataBag result = (DataBag)fn.invoke(tupleEntry.getTuple());
    System.out.println("tupleEntry = " + tupleEntry);
    System.out.println("result = " + result);
    functionCall.getOutputCollector().add(new Tuple(result));
  }
}
