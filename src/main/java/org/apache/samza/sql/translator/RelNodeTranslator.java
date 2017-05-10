package org.apache.samza.sql.translator;

import org.apache.calcite.rel.RelNode;

public interface RelNodeTranslator<T extends RelNode>  {
  void translate(T node, TranslatorContext context);
}
