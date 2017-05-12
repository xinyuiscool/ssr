package org.apache.samza.sql.planner;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;


public class QueryPlanner {
  private static final String STREAM_SCHEMA =  "streams.%s.schema";
  private static final Logger log = LoggerFactory.getLogger(QueryPlanner.class);

  public static RelRoot plan(String query, Config config) {
    try {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();


      Collection<String> streams = JavaConversions.asJavaCollection(new StreamConfig(config).getStreamIds());
      for (String stream : streams) {
        String schemaCfg = String.format(STREAM_SCHEMA, stream);
        if (config.containsKey(schemaCfg)) {
          // this is a sql input stream
          TableFactory<Table> tableFactory = ClassLoaderHelper.fromClassName(config.get(schemaCfg));
          Table table = tableFactory.create(rootSchema, stream, Collections.EMPTY_MAP, null);
          rootSchema.add(stream, table);
        }
      };

      final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();

      traitDefs.add(ConventionTraitDef.INSTANCE);
      traitDefs.add(RelCollationTraitDef.INSTANCE);

      FrameworkConfig frameworkConfig =
          Frameworks.newConfigBuilder().parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL).build())
              .defaultSchema(rootSchema).operatorTable(SqlStdOperatorTable.instance()) // TODO: Implement Samza specific operator table
              .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT)
              //.ruleSets(SamzaRuleSets.getRuleSets())
              .costFactory(null)
                  //.typeSystem(SamzaRelDataTypeSystem.SAMZA_REL_DATATYPE_SYSTEM)
              .build();
      Planner planner = Frameworks.getPlanner(frameworkConfig);

      SqlNode sql = planner.parse(query);
      SqlNode validatedSql = planner.validate(sql);
      RelRoot relRoot = planner.rel(validatedSql);
      log.info("query plan:\n" + sql.toString());

      log.info("relational graph:");
      traverse(relRoot.project());
      //TODO: translate from rel graph to samza stream graph
      return relRoot;
    } catch (Exception e) {
      throw  new SamzaException(e);
    }
  }

  private static void traverse(RelNode node) {
    StringBuilder builder = new StringBuilder(node.getRelTypeName());
    if (!node.getInputs().isEmpty()) {
      builder.append(" <- ");
      List<String> inputs = node.getInputs().stream().map(RelNode::getRelTypeName).collect(Collectors.toList());
      builder.append(Joiner.on(',').join(inputs));
    }
    log.info(builder.toString());

    node.getInputs().stream().forEach(n -> traverse(n));
  }
}
