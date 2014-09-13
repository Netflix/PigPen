package pigpen.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.TupleEntry;

public class PigPenFunction extends BaseOperation implements Function {
  @Override
  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    TupleEntry tupleEntry = functionCall.getArguments();
    System.out.println("tupleEntry = " + tupleEntry);
    functionCall.getOutputCollector().add(tupleEntry.getTuple());
  }
}
