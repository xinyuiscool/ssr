package org.apache.samza.sql.translator;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.TupleMessage;


public class ScanTranslator implements RelNodeTranslator<TableScan> {

  public void translate(final TableScan tableScan, final TranslatorContext context) {
    StreamGraph streamGraph = context.getStreamGraph();
    String tableName = tableScan.getTable().getQualifiedName().get(0);

    MessageStream<TupleMessage> inputStream = streamGraph.getInputStream(tableName, (String k, Object v) -> {
      RelDataType type = tableScan.getRowType();
      return TupleMessage.fromReflection(v, type);
    });

    context.registerMessageStream(tableScan.getId(), inputStream);
  }
}
