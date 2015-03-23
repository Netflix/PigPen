/*
 *
 *  Copyright 2015 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

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
