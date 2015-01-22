package pigpen.cascading;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class RankBuffer extends BaseOperation implements Buffer {

  public RankBuffer(Fields fields) {
    super(fields);
  }

  @Override
  public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
    int rank = 0;
    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
    while (iterator.hasNext()) {
      TupleEntry entry = iterator.next();
      bufferCall.getOutputCollector().add(new Tuple(rank++, entry.getObject(0)));
    }
  }
}
