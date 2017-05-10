package org.apache.samza.sql.translator;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.TupleMessage;


public class ProjectTranslator implements RelNodeTranslator<Project> {

  public void translate(final Project project, final TranslatorContext context) {
    MessageStream<TupleMessage> inputStream = context.getMessageStream(project.getInput().getId());
    MessageStream<TupleMessage> outputStream = inputStream.map(m -> {
      RelDataType type = project.getRowType();
      Expression expr = context.getExpressionCompiler().compile(project.getInputs(), project.getProjects());
      Object[] output = new Object[type.getFieldCount()];
      expr.execute(m.getValue(), output);

      return new TupleMessage(output);
    });

    context.registerMessageStream(project.getId(), outputStream);
  }
}
