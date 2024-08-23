package com.starrocks.sql.optimizer.operator.physical;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// todo extends PhysicalOlapScanOperator ?
public class PhysicalExternalOlapScanOperator extends PhysicalScanOperator  {
    private DistributionSpec distributionSpec;
    private long selectedIndexId;
    private List<Long> selectedTabletId;
    private List<Long> hintsReplicaId;
    private List<Long> selectedPartitionId;

    private boolean isPreAggregation;
    private String turnOffReason;
    protected boolean needSortedByKeyPerTablet = false;
    protected boolean needOutputChunkByBucket = false;
    protected boolean withoutColocateRequirement = false;

    private boolean usePkIndex = false;

    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
    private Map<Integer, ScalarOperator> globalDictsExpr = Maps.newHashMap();

    // Rewriting the scan column ref also needs to rewrite the pruned predicate at the same time.
    private List<ScalarOperator> prunedPartitionPredicates = Lists.newArrayList();

    private long gtid = 0;

    private PhysicalExternalOlapScanOperator() {
        super(OperatorType.PHYSICAL_EXTERNAL_OLAP_SCAN);
    }

    public PhysicalExternalOlapScanOperator(Table table,
                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                    DistributionSpec distributionDesc,
                                    long limit,
                                    ScalarOperator predicate,
                                    long selectedIndexId,
                                    List<Long> selectedPartitionId,
                                    List<Long> selectedTabletId,
                                    List<Long> hintsReplicaId,
                                    List<ScalarOperator> prunedPartitionPredicates,
                                    Projection projection,
                                    boolean usePkIndex) {
        super(OperatorType.PHYSICAL_OLAP_SCAN, table, colRefToColumnMetaMap, limit, predicate, projection);
        this.distributionSpec = distributionDesc;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.selectedTabletId = selectedTabletId;
        this.hintsReplicaId = hintsReplicaId;
        this.prunedPartitionPredicates = prunedPartitionPredicates;
        this.usePkIndex = usePkIndex;
    }

    public PhysicalExternalOlapScanOperator(LogicalExternalOlapScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_OLAP_SCAN, scanOperator);
        this.distributionSpec = scanOperator.getDistributionSpec();
        this.selectedIndexId = scanOperator.getSelectedIndexId();
        this.gtid = scanOperator.getGtid();
        this.selectedPartitionId = scanOperator.getSelectedPartitionId();
        this.selectedTabletId = scanOperator.getSelectedTabletId();
        this.hintsReplicaId = scanOperator.getHintsReplicaIds();
        this.prunedPartitionPredicates = scanOperator.getPrunedPartitionPredicates();
        this.usePkIndex = scanOperator.isUsePkIndex();
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public long getGtid() {
        return gtid;
    }

    public void setSelectedPartitionId(List<Long> selectedPartitionId) {
        this.selectedPartitionId = selectedPartitionId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public void setSelectedTabletId(List<Long> tabletId) {
        this.selectedTabletId = tabletId;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public List<Long> getHintsReplicaId() {
        return hintsReplicaId;
    }

    public boolean isPreAggregation() {
        return isPreAggregation;
    }

    public void setPreAggregation(boolean preAggregation) {
        isPreAggregation = preAggregation;
    }

    public String getTurnOffReason() {
        return turnOffReason;
    }

    public void setTurnOffReason(String turnOffReason) {
        this.turnOffReason = turnOffReason;
    }

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(List<Pair<Integer, ColumnDict>> globalDicts) {
        this.globalDicts = globalDicts;
    }

    public Map<Integer, ScalarOperator> getGlobalDictsExpr() {
        return globalDictsExpr;
    }

    public List<ScalarOperator> getPrunedPartitionPredicates() {
        return prunedPartitionPredicates;
    }

    public void setOutputColumns(List<ColumnRefOperator> outputColumns) {
        this.outputColumns = outputColumns;
    }

    public boolean needSortedByKeyPerTablet() {
        return needSortedByKeyPerTablet;
    }

    public boolean needOutputChunkByBucket() {
        return needOutputChunkByBucket;
    }

    public void setNeedSortedByKeyPerTablet(boolean needSortedByKeyPerTablet) {
        this.needSortedByKeyPerTablet = needSortedByKeyPerTablet;
    }

    public void setNeedOutputChunkByBucket(boolean needOutputChunkByBucket) {
        this.needOutputChunkByBucket = needOutputChunkByBucket;
    }

    public boolean isWithoutColocateRequirement() {
        return withoutColocateRequirement;
    }

    public void setWithoutColocateRequirement(boolean withoutColocateRequirement) {
        this.withoutColocateRequirement = withoutColocateRequirement;
    }

    public boolean isUsePkIndex() {
        return usePkIndex;
    }

    @Override
    public String toString() {
        return "PhysicalExternalOlapScan" + " {" +
                "table='" + table.getId() + '\'' +
                ", outputColumns='" + getOutputColumns() + '\'' +
                '}';
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalExternalOlapScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalExternalOlapScan(optExpression, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), selectedIndexId, selectedPartitionId,
                selectedTabletId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalExternalOlapScanOperator that = (PhysicalExternalOlapScanOperator) o;
        return selectedIndexId == that.selectedIndexId &&
                gtid == that.gtid &&
                Objects.equals(distributionSpec, that.distributionSpec) &&
                Objects.equals(selectedPartitionId, that.selectedPartitionId) &&
                Objects.equals(selectedTabletId, that.selectedTabletId);
    }

    public DistributionSpec getDistributionSpec() {
        // In UT, the distributionInfo may be null
        if (distributionSpec != null) {
            return distributionSpec;
        } else {
            // 1023 is a placeholder column id, only in order to pass UT
            HashDistributionDesc leftHashDesc = new HashDistributionDesc(Collections.singletonList(1023),
                    HashDistributionDesc.SourceType.LOCAL);
            return DistributionSpec.createHashDistributionSpec(leftHashDesc);
        }
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return true;
    }

    public static PhysicalExternalOlapScanOperator.Builder builder() {
        return new PhysicalExternalOlapScanOperator.Builder();
    }

    public static class Builder
            extends PhysicalScanOperator.Builder<PhysicalExternalOlapScanOperator, PhysicalExternalOlapScanOperator.Builder> {
        @Override
        protected PhysicalExternalOlapScanOperator newInstance() {
            return new PhysicalExternalOlapScanOperator();
        }

        @Override
        public PhysicalExternalOlapScanOperator.Builder withOperator(PhysicalExternalOlapScanOperator operator) {
            super.withOperator(operator);
            builder.distributionSpec = operator.distributionSpec;
            builder.selectedIndexId = operator.selectedIndexId;
            builder.gtid = operator.gtid;
            builder.selectedTabletId = operator.selectedTabletId;
            builder.hintsReplicaId = operator.hintsReplicaId;
            builder.selectedPartitionId = operator.selectedPartitionId;

            builder.isPreAggregation = operator.isPreAggregation;
            builder.turnOffReason = operator.turnOffReason;
            builder.needSortedByKeyPerTablet = operator.needSortedByKeyPerTablet;
            builder.needOutputChunkByBucket = operator.needOutputChunkByBucket;
            builder.usePkIndex = operator.usePkIndex;
            builder.globalDicts = operator.globalDicts;
            builder.prunedPartitionPredicates = operator.prunedPartitionPredicates;
            return this;
        }

        public PhysicalExternalOlapScanOperator.Builder setGlobalDicts(List<Pair<Integer, ColumnDict>> globalDicts) {
            builder.globalDicts = globalDicts;
            return this;
        }

        public PhysicalExternalOlapScanOperator.Builder setGlobalDictsExpr(Map<Integer, ScalarOperator> globalDictsExpr) {
            builder.globalDictsExpr = globalDictsExpr;
            return this;
        }

        public PhysicalExternalOlapScanOperator.Builder setPrunedPartitionPredicates(List<ScalarOperator> prunedPartitionPredicates) {
            builder.prunedPartitionPredicates = prunedPartitionPredicates;
            return this;
        }
    }

}
