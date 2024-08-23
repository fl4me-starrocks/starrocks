package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptExternalOlapDistributionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;


/**
 * This class need to run after PartitionPruneRule
 * This class actually Prune the Olap table tablet ids.
 * <p>
 * Dependency predicate push down scan node
 */
public class ExternalOlapDistributionPruneRule extends TransformationRule {

    private static final Logger LOG = LogManager.getLogger(DistributionPruneRule.class);

    public ExternalOlapDistributionPruneRule() {
        super(RuleType.TF_DISTRIBUTION_PRUNE, Pattern.create(OperatorType.LOGICAL_EXTERNAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalExternalOlapScanOperator externalOlapScanOperator = (LogicalExternalOlapScanOperator) input.getOp();
        Preconditions.checkState(externalOlapScanOperator.getHintsTabletIds() != null);
        List<Long> result;
        if (!externalOlapScanOperator.getHintsTabletIds().isEmpty()) {
            result = externalOlapScanOperator.getHintsTabletIds();
        } else {
            result = OptExternalOlapDistributionPruner.pruneTabletIds(externalOlapScanOperator,
                    externalOlapScanOperator.getSelectedPartitionId());
        }
        if (result.equals(externalOlapScanOperator.getSelectedTabletId())) {
            return Collections.emptyList();
        }

        LogicalExternalOlapScanOperator.Builder builder = new LogicalExternalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(externalOlapScanOperator).setSelectedTabletId(result).build(),
                input.getInputs()));
    }
}
