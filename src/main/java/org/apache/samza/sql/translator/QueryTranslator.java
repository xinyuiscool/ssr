package org.apache.samza.sql.translator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.sql.planner.QueryPlanner;


public class QueryTranslator {

  public static void translate(String sql, StreamGraph streamGraph) {
    final RelRoot relRoot = QueryPlanner.plan(sql);
    final TranslatorContext context = new TranslatorContext(streamGraph, relRoot);
    final RelNode node = relRoot.project();

    node.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelNode node = super.visit(scan);
        new ScanTranslator().translate(scan, context);
        return node;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode node = super.visit(project);
        new ProjectTranslator().translate(project, context);
        return node;
      }
    });
  }
}
