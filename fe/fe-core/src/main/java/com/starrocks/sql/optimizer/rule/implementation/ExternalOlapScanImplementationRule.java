package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class ExternalOlapScanImplementationRule extends ImplementationRule {

    public ExternalOlapScanImplementationRule() {
        super(RuleType.IMP_EOLAP_LSCAN_TO_PSCAN,
                Pattern.create(OperatorType.LOGICAL_EXTERNAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalExternalOlapScanOperator scan = (LogicalExternalOlapScanOperator) input.getOp();
        PhysicalExternalOlapScanOperator physicalExternalOlapScanOperator = new PhysicalExternalOlapScanOperator(scan);

        physicalExternalOlapScanOperator.setSalt(scan.getSalt());
        physicalExternalOlapScanOperator.setColumnAccessPaths(scan.getColumnAccessPaths());
        OptExpression result = new OptExpression(physicalExternalOlapScanOperator);
        return Lists.newArrayList(result);
    }
}
