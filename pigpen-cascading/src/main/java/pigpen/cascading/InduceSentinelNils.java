package pigpen.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import clojure.lang.IFn;

public class InduceSentinelNils extends BaseOperation implements Function {

    private static final IFn INDUCE_NILS = OperationUtil.getVar("induce-sentinel-nil");

    private final Integer index;

    public InduceSentinelNils(final Integer index, final Fields fields) {
        super(fields);
        this.index = index;
    }

    @Override
    public void operate(final FlowProcess flowProcess, final FunctionCall functionCall) {
        INDUCE_NILS.invoke(functionCall, this.index);
    }
}
