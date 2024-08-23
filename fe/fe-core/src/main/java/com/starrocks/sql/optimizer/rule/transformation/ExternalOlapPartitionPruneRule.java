package com.starrocks.sql.optimizer.rule.transformation;


import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptExternalOlapPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * This class does:
 * 1. Prune the External Olap table partition ids, Dependency predicates push down scan node
 * 2. Prune predicate if the data of partitions meets the predicate, to avoid execute predicate.
 */
public class ExternalOlapPartitionPruneRule extends TransformationRule{

    public ExternalOlapPartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_EXTERNAL_OLAP_SCAN));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        // if the partition id is already selected, no need to prune again
        if (Utils.isOpAppliedRule(op, Operator.OP_PARTITION_PRUNE_BIT)) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalExternalOlapScanOperator logicalExternalOlapScanOperator = (LogicalExternalOlapScanOperator) input.getOp();
        LogicalExternalOlapScanOperator prunedExternalOlapScanOperator = null;
        if (logicalExternalOlapScanOperator.getSelectedPartitionId() == null) {
            prunedExternalOlapScanOperator = OptExternalOlapPartitionPruner.prunePartitions(logicalExternalOlapScanOperator);
        } else {
            // do merge pruned partitions with new pruned partitions
            prunedExternalOlapScanOperator = OptExternalOlapPartitionPruner.mergePartitionPrune(logicalExternalOlapScanOperator);
        }
        Utils.setOpAppliedRule(prunedExternalOlapScanOperator, Operator.OP_PARTITION_PRUNE_BIT);
        return Lists.newArrayList(OptExpression.create(prunedExternalOlapScanOperator, input.getInputs()));
    }
}
