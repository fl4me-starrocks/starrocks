package com.starrocks.sql.optimizer.rewrite;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.DistributionPruner;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OptExternalOlapDistributionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalOlapDistributionPruner.class);

    public static List<Long> pruneTabletIds(LogicalExternalOlapScanOperator externalOlapScanOperator,
                                            List<Long> selectedPartitionIds) {
        ExternalOlapTable externalOlapTable = (ExternalOlapTable) externalOlapScanOperator.getTable();

        List<Long> result = Lists.newArrayList();
        for (Long partitionId : selectedPartitionIds) {
            Partition partition = externalOlapTable.getPartition(partitionId);
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                MaterializedIndex table = physicalPartition.getIndex(externalOlapScanOperator.getSelectedIndexId());
                Collection<Long> tabletIds = distributionPrune(table, partition.getDistributionInfo(),
                        externalOlapScanOperator, externalOlapTable.getIdToColumn());
                result.addAll(tabletIds);
            }
        }
        return result;
    }

    private static Collection<Long> distributionPrune(MaterializedIndex index, DistributionInfo distributionInfo,
                                                      LogicalExternalOlapScanOperator externalOlapScanOperator, Map<ColumnId, Column> idToColumn) {
        try {
            DistributionPruner distributionPruner;
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
                Table table = externalOlapScanOperator.getTable();
                if (table.isExprPartitionTable()) {
                    // Bucketing needs to use the original predicate for hashing
                } else {
                    filters = externalOlapScanOperator.getColumnFilters();
                }
                distributionPruner = new HashDistributionPruner(index.getTabletIdsInOrder(),
                        MetaUtils.getColumnsByColumnIds(idToColumn, info.getDistributionColumns()),
                        filters,
                        info.getBucketNum());
                return distributionPruner.prune();
            }

        } catch (AnalysisException e) {
            LOG.warn("distribution prune failed. ", e);
        }

        return index.getTabletIdsInOrder();
    }
}
