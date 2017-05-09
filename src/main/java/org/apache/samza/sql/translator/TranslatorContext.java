package org.apache.samza.sql.translator;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.data.RexToJavaCompiler;


public class TranslatorContext {
  private final StreamGraph streamGraph;
  private final Map<Integer, MessageStream> messsageStreams = new HashMap<>();
  private final RexToJavaCompiler compiler;

  public TranslatorContext(StreamGraph streamGraph, RelRoot relRoot) {
    this.streamGraph = streamGraph;
    this.compiler = createExpressionCompiler(relRoot);
  }

  private RexToJavaCompiler createExpressionCompiler(RelRoot relRoot) {
    RelDataTypeFactory dataTypeFactory = relRoot.project().getCluster().getTypeFactory();
    RexBuilder rexBuilder = new RexBuilder(dataTypeFactory);
    return new RexToJavaCompiler(rexBuilder);
  }

  public RexToJavaCompiler getExpressionCompiler() {
    return compiler;
  }

  public void registerMessageStream(int id, MessageStream stream) {
    messsageStreams.put(id, stream);
  }

  public MessageStream getMessageStream(int id) {
    return messsageStreams.get(id);
  }

}
