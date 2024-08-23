package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Objects;

// todo extends LogicalOlapScanOperator
public class LogicalExternalOlapScanOperator extends LogicalScanOperator {

    private DistributionSpec distributionSpec;
    private long selectedIndexId;
    private List<Long> selectedPartitionId;
    private PartitionNames partitionNames;
    private boolean hasTableHints;
    private List<Long> selectedTabletId;
    private List<Long> hintsTabletIds;
    private List<Long> hintsReplicaIds;

    private List<ScalarOperator> prunedPartitionPredicates;
    private boolean usePkIndex;

    // record if this scan is derived from SplitScanORToUnionRule
    private boolean fromSplitOR;

    private long gtid = 0;

    // Only for UT
    public LogicalExternalOlapScanOperator(Table table) {
        this(table, Maps.newHashMap(), Maps.newHashMap(), null, Operator.DEFAULT_LIMIT, null);
    }

    public LogicalExternalOlapScanOperator(
            Table table,
            Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
            Map<Column, ColumnRefOperator> columnMetaToColRefMap,
            DistributionSpec distributionSpec,
            long limit,
            ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, distributionSpec, limit, predicate,
                ((ExternalOlapTable) table).getBaseIndexId(),
                null,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);
    }

    public LogicalExternalOlapScanOperator(
        Table table,
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
        Map<Column, ColumnRefOperator> columnMetaToColRefMap,
        DistributionSpec distributionSpec,
        long limit,
        ScalarOperator predicate,
        long selectedIndexId,
        List<Long> selectedPartitionId,
        PartitionNames partitionNames,
        boolean hasTableHints,
        List<Long> selectedTabletId,
        List<Long> hintsTabletIds,
        List<Long> hintsReplicaIds,
        boolean usePkIndex
    ) {
        super(OperatorType.LOGICAL_EXTERNAL_OLAP_SCAN, table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, null);
        Preconditions.checkState(table instanceof ExternalOlapTable);
        this.distributionSpec = distributionSpec;
        this.selectedIndexId = selectedIndexId;
        this.selectedPartitionId = selectedPartitionId;
        this.partitionNames = partitionNames;
        this.hasTableHints = hasTableHints;
        this.selectedTabletId = selectedTabletId;
        this.hintsTabletIds = hintsTabletIds;
        this.hintsReplicaIds = hintsReplicaIds;
        this.prunedPartitionPredicates = Lists.newArrayList();
        this.usePkIndex = usePkIndex;
    }

    private LogicalExternalOlapScanOperator() {
        super(OperatorType.LOGICAL_EXTERNAL_OLAP_SCAN);
        this.prunedPartitionPredicates = ImmutableList.of();
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
    }

    public List<Long> getSelectedPartitionId() {
        return selectedPartitionId;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public List<Long> getSelectedTabletId() {
        return selectedTabletId;
    }

    public long getGtid() {
        return gtid;
    }

    @Override
    public boolean isEmptyOutputRows() {
        return selectedTabletId == null || selectedTabletId.isEmpty() ||
                selectedPartitionId == null || selectedPartitionId.isEmpty();
    }

    public List<Long> getHintsTabletIds() {
        return hintsTabletIds;
    }

    public List<Long> getHintsReplicaIds() {
        return hintsReplicaIds;
    }

    public boolean hasTableHints() {
        return hasTableHints;
    }

    public boolean isUsePkIndex() {
        return usePkIndex;
    }

    public List<ScalarOperator> getPrunedPartitionPredicates() {
        return prunedPartitionPredicates;
    }

    public boolean isFromSplitOR() {
        return fromSplitOR;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalExternalOlapScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalExternalOlapScanOperator that = (LogicalExternalOlapScanOperator) o;
        return selectedIndexId == that.selectedIndexId &&
                gtid == that.gtid &&
                Objects.equals(distributionSpec, that.distributionSpec) &&
                Objects.equals(selectedPartitionId, that.selectedPartitionId) &&
                Objects.equals(partitionNames, that.partitionNames) &&
                Objects.equals(selectedTabletId, that.selectedTabletId) &&
                Objects.equals(hintsTabletIds, that.hintsTabletIds) &&
                Objects.equals(hintsReplicaIds, that.hintsReplicaIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), selectedIndexId, gtid, selectedPartitionId,
                selectedTabletId, hintsTabletIds, hintsReplicaIds);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalExternalOlapScanOperator, LogicalExternalOlapScanOperator.Builder> {

        @Override
        protected LogicalExternalOlapScanOperator newInstance() {
            return new LogicalExternalOlapScanOperator();
        }

        @Override
        public Builder withOperator(LogicalExternalOlapScanOperator scanOperator) {
            super.withOperator(scanOperator);

            builder.distributionSpec = scanOperator.distributionSpec;
            builder.selectedIndexId = scanOperator.selectedIndexId;
            builder.gtid = scanOperator.gtid;
            builder.selectedPartitionId = scanOperator.selectedPartitionId;
            builder.partitionNames = scanOperator.partitionNames;
            builder.hasTableHints = scanOperator.hasTableHints;
            builder.selectedTabletId = scanOperator.selectedTabletId;
            builder.hintsTabletIds = scanOperator.hintsTabletIds;
            builder.hintsReplicaIds = scanOperator.hintsReplicaIds;
            builder.prunedPartitionPredicates = scanOperator.prunedPartitionPredicates;
            builder.usePkIndex = scanOperator.usePkIndex;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setSelectedIndexId(long selectedIndexId) {
            builder.selectedIndexId = selectedIndexId;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setGtid(long gtid) {
            builder.gtid = gtid;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setSelectedTabletId(List<Long> selectedTabletId) {
            builder.selectedTabletId = ImmutableList.copyOf(selectedTabletId);
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setSelectedPartitionId(List<Long> selectedPartitionId) {
            if (selectedPartitionId == null) {
                builder.selectedPartitionId = null;
            } else {
                builder.selectedPartitionId = ImmutableList.copyOf(selectedPartitionId);
            }
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setPrunedPartitionPredicates(List<ScalarOperator> prunedPartitionPredicates) {
            builder.prunedPartitionPredicates = ImmutableList.copyOf(prunedPartitionPredicates);
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setDistributionSpec(DistributionSpec distributionSpec) {
            builder.distributionSpec = distributionSpec;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setPartitionNames(PartitionNames partitionNames) {
            builder.partitionNames = partitionNames;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setHintsTabletIds(List<Long> hintsTabletIds) {
            builder.hintsTabletIds = ImmutableList.copyOf(hintsTabletIds);
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setHintsReplicaIds(List<Long> hintsReplicaIds) {
            builder.hintsReplicaIds = ImmutableList.copyOf(hintsReplicaIds);
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setHasTableHints(boolean hasTableHints) {
            builder.hasTableHints = hasTableHints;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setFromSplitOR(boolean fromSplitOR) {
            builder.fromSplitOR = fromSplitOR;
            return this;
        }

        public LogicalExternalOlapScanOperator.Builder setUsePkIndex(boolean usePkIndex) {
            builder.usePkIndex = usePkIndex;
            return this;
        }
    }


}
