package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.logical.LogicalExternalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class OptExternalOlapPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalOlapPartitionPruner.class);

    public static LogicalExternalOlapScanOperator prunePartitions(LogicalExternalOlapScanOperator logicalExternalOlapScanOperator) {
        List<Long> selectedPartitionIds = null;
        ExternalOlapTable table = (ExternalOlapTable) logicalExternalOlapScanOperator.getTable();
        PartitionInfo partitionInfo = table.getPartitionInfo();

        if (partitionInfo.isRangePartition()) {
            selectedPartitionIds = rangePartitionPrune(table, (RangePartitionInfo) partitionInfo, logicalExternalOlapScanOperator);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds = listPartitionPrune(table, (ListPartitionInfo) partitionInfo, logicalExternalOlapScanOperator);
        }
        if (selectedPartitionIds == null) {
            selectedPartitionIds =
                    table.getPartitions().stream().filter(Partition::hasData).map(Partition::getId).collect(Collectors.toList());
            // some test cases need to perceive partitions pruned, so we can not filter empty partitions.
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> table.getPartition(id).hasData()).collect(Collectors.toList());
        }

        if (isNeedFurtherPrune(selectedPartitionIds, logicalExternalOlapScanOperator, partitionInfo)) {
            List<Column> partitionColumns = partitionInfo.getPartitionColumns(table.getIdToColumn());
            PartitionColPredicateExtractor extractor = new PartitionColPredicateExtractor(
                    partitionColumns,
                    (RangePartitionInfo)partitionInfo,
                    logicalExternalOlapScanOperator.getColumnMetaToColRefMap()
            );
            PartitionColPredicateEvaluator evaluator = new PartitionColPredicateEvaluator(
                    partitionColumns,
                    (RangePartitionInfo) partitionInfo,
                    selectedPartitionIds
            );
            selectedPartitionIds = evaluator.prunePartitions(extractor, logicalExternalOlapScanOperator.getPredicate());
        }

        final Pair<ScalarOperator, List<ScalarOperator>> prunePartitionPredicate =
                prunePartitionPredicates(logicalExternalOlapScanOperator, selectedPartitionIds);

        final LogicalExternalOlapScanOperator.Builder builder = new LogicalExternalOlapScanOperator.Builder();
        builder.withOperator(logicalExternalOlapScanOperator)
                .setSelectedPartitionId(selectedPartitionIds);

        if (prunePartitionPredicate != null) {
            builder.setPredicate(Utils.compoundAnd(prunePartitionPredicate.first))
                    .setPrunedPartitionPredicates(prunePartitionPredicate.second);
        }
        return builder.build();
    }

    /**
     * If the selected partition ids are not null, merge the selected partition ids and do further partition pruning.
     */
    public static LogicalExternalOlapScanOperator mergePartitionPrune(LogicalExternalOlapScanOperator logicalExternalOlapScanOperator) {
        // already pruned partitions
        List<Long> selectedPartitionIds = logicalExternalOlapScanOperator.getSelectedPartitionId();
        // new pruned partitions
        LogicalExternalOlapScanOperator newOlapScanOperator = OptExternalOlapPartitionPruner.prunePartitions(logicalExternalOlapScanOperator);
        List<Long> newSelectedPartitionIds = newOlapScanOperator.getSelectedPartitionId();
        // merge selected partition ids
        List<Long> ansPartitionIds = null;
        if (newSelectedPartitionIds != null && selectedPartitionIds != null) {
            ansPartitionIds = Lists.newArrayList(selectedPartitionIds);
            // use hash set to accelerate the intersection operation
            ansPartitionIds.retainAll(new HashSet<>(newSelectedPartitionIds));
        } else {
            ansPartitionIds = (selectedPartitionIds == null) ? newSelectedPartitionIds : selectedPartitionIds;
        }

        final LogicalExternalOlapScanOperator.Builder builder = new LogicalExternalOlapScanOperator.Builder();
        builder.withOperator(newOlapScanOperator)
                .setSelectedPartitionId(ansPartitionIds)
                // new predicate should cover the old one
                .setPredicate(newOlapScanOperator.getPredicate())
                // use the new pruned partition predicates
                .setPrunedPartitionPredicates(newOlapScanOperator.getPrunedPartitionPredicates());
        return builder.build();
    }

    private static Pair<ScalarOperator, List<ScalarOperator>> prunePartitionPredicates(
            LogicalExternalOlapScanOperator logicalExternalOlapScanOperator, List<Long> selectedPartitionIds
    ) {
        List<ScalarOperator> scanPredicates = Utils.extractConjuncts(logicalExternalOlapScanOperator.getPredicate());

        ExternalOlapTable table = (ExternalOlapTable) logicalExternalOlapScanOperator.getTable();
        PartitionInfo tablePartitionInfo = table.getPartitionInfo();
        if (!tablePartitionInfo.isRangePartition()) {
            return null;
        }

        RangePartitionInfo partitionInfo = (RangePartitionInfo) tablePartitionInfo;
        if (partitionInfo.getPartitionColumnsSize() != 1 || selectedPartitionIds.isEmpty()) {
            return null;
        }
        List<ScalarOperator> prunedPartitionPredicates = Lists.newArrayList();
        Map<String, PartitionColumnFilter> predicateRangeMap = Maps.newHashMap();

        String columnName = partitionInfo.getPartitionColumns(table.getIdToColumn()).get(0).getName();
        Column column = logicalExternalOlapScanOperator.getTable().getColumn(columnName);

        List<Range<PartitionKey>> partitionRanges =
                selectedPartitionIds.stream().map(partitionInfo::getRange).collect(Collectors.toList());

        // we convert range to [minRange, maxRange]
        PartitionKey minRange =
                Collections.min(partitionRanges.stream().map(range -> {
                    PartitionKey lower = range.lowerEndpoint();
                    if (range.contains(lower)) {
                        return lower;
                    } else {
                        return lower.successor();
                    }
                }).collect(Collectors.toList()));
        PartitionKey maxRange =
                Collections.max(partitionRanges.stream().map(range -> {
                    PartitionKey upper = range.upperEndpoint();
                    if (range.contains(upper)) {
                        return upper;
                    } else {
                        return upper.predecessor();
                    }
                }).collect(Collectors.toList()));

        for (ScalarOperator predicate : scanPredicates) {
            if (!Utils.containColumnRef(predicate, columnName)) {
                continue;
            }

            predicateRangeMap.clear();
            // todo mark we put table to a function
            ColumnFilterConverter.convertColumnFilter(predicate, predicateRangeMap, table);

            if (predicateRangeMap.isEmpty()) {
                continue;
            }

            PartitionColumnFilter pcf = predicateRangeMap.get(columnName);

            // In predicate don't support predicate prune
            if (null != pcf.getInPredicateLiterals()) {
                continue;
            }

            // None/Null bound predicate can't prune
            LiteralExpr lowerBound = pcf.getLowerBound();
            LiteralExpr upperBound = pcf.getUpperBound();

            if ((null == lowerBound || lowerBound.isConstantNull()) &&
            (null == upperBound || upperBound.isConstantNull())) {
                continue;
            }

            boolean lowerBind = true;
            boolean upperBind = true;
            if (null != lowerBound) {
                lowerBind = false;
                PartitionKey min = new PartitionKey();
                min.pushColumn(pcf.getLowerBound(), column.getPrimitiveType());
                int cmp = minRange.compareTo(min);
                if (cmp > 0 || (0 == cmp && pcf.lowerBoundInclusive)) {
                    lowerBind = true;
                }
            }

            if (null != upperBound) {
                upperBind = false;
                PartitionKey max = new PartitionKey();
                max.pushColumn(upperBound, column.getPrimitiveType());
                int cmp = maxRange.compareTo(max);
                if (cmp < 0 || (0 == cmp && pcf.upperBoundInclusive)) {
                    upperBind = true;
                }
            }

            if (lowerBind && upperBind) {
                prunedPartitionPredicates.add(predicate);
            }
        }

        if (prunedPartitionPredicates.isEmpty()) {
            return null;
        }

        scanPredicates.removeAll(prunedPartitionPredicates);

        if (column.isAllowNull() && containsNullValue(minRange)
        && !checkFilterNullValue(scanPredicates, logicalExternalOlapScanOperator.getPredicate().clone())) {
            return null;
        }

        return Pair.create(Utils.compoundAnd(scanPredicates), prunedPartitionPredicates);

    }

    private static void putValueMapItem(ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds,
                                        Long partitionId,
                                        LiteralExpr value
                                        ) {
        Set<Long> partitionIdSet = partitionValueToIds.get(value);
        if (partitionIdSet == null) {
            partitionIdSet = Sets.newHashSet();
        }
        partitionIdSet.add(partitionId);
        partitionValueToIds.put(value, partitionIdSet);
    }

    private static List<Long> listPartitionPrune(ExternalOlapTable externalOlapTable, ListPartitionInfo listPartitionInfo,
                                                 LogicalExternalOlapScanOperator logicalExternalOlapScanOperator) {
        Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValueMap =
                Maps.newConcurrentMap();
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = new HashMap<>();

        // Currently queries either specify a temporary partition, or do not. There is no situation
        // where two partitions are checked at the same time
        boolean isTemporaryPartitionPrune = false;
        List<Long> specifyPartitionIds = null;
        // single item list partition has only once column mapper
        Map<Long, List<LiteralExpr>> literalExprValuesMap = listPartitionInfo.getLiteralExprValues();
        Set<Long> partitionIds = Sets.newHashSet();
        if (logicalExternalOlapScanOperator.getPartitionNames() != null) {
            for (String partName : logicalExternalOlapScanOperator.getPartitionNames().getPartitionNames()) {
                boolean isTemp = logicalExternalOlapScanOperator.getPartitionNames().isTemp();
                if (isTemp) {
                    isTemporaryPartitionPrune = true;
                }
                Partition part = externalOlapTable.getPartition(partName, isTemp);
                if (part == null) {
                    continue;
                }
                partitionIds.add(part.getId());
            }
            specifyPartitionIds = Lists.newArrayList(partitionIds);
        } else {
            partitionIds = Sets.newHashSet(listPartitionInfo.getPartitionIds(false));
        }

        List<Column> partitionColumns = listPartitionInfo.getPartitionColumns(externalOlapTable.getIdToColumn());
        if (literalExprValuesMap != null && !literalExprValuesMap.isEmpty()) {
            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
            for (Map.Entry<Long, List<LiteralExpr>> entry : literalExprValuesMap.entrySet()) {
                Long partitionId = entry.getKey();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }
                List<LiteralExpr> values = entry.getValue();
                if (values == null || values.isEmpty()) {
                    continue;
                }
                values.forEach(value -> putValueMapItem(partitionValueToIds, partitionId, value));
            }
            // single item list partition has only one column
            Column column = partitionColumns.get(0);
            ColumnRefOperator columnRefOperator = logicalExternalOlapScanOperator.getColumnReference(column);
            columnToPartitionValueMap.put(columnRefOperator, partitionValueToIds);
            columnToNullPartitions.put(columnRefOperator, new HashSet<>());
        }

        // multiItem list partition mapper
        Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = listPartitionInfo.getMultiLiteralExprValues();
        if (multiLiteralExprValues != null && !multiLiteralExprValues.isEmpty()) {
            for (int i = 0; i < partitionColumns.size(); i++) {
                ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
                Set<Long> nullPartitionIds = new HashSet<>();
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()) {
                    Long partitionId = entry.getKey();
                    if (!partitionIds.contains(partitionId)) {
                        continue;
                    }
                    List<List<LiteralExpr>> multiValues = entry.getValue();
                    if (multiValues == null || multiValues.isEmpty()) {
                        continue;
                    }
                    for (List<LiteralExpr> values : multiValues) {
                        LiteralExpr value = values.get(i);
                        // store null partition value seperated from non-null partition values
                        if (value.isConstantNull()) {
                            nullPartitionIds.add(partitionId);
                        } else {
                            putValueMapItem(partitionValueToIds, partitionId, value);
                        }
                    }
                }
                Column column = partitionColumns.get(i);
                ColumnRefOperator columnRefOperator = logicalExternalOlapScanOperator.getColumnReference(column);
                columnToPartitionValueMap.put(columnRefOperator, partitionValueToIds);
                columnToNullPartitions.put(columnRefOperator, nullPartitionIds);
            }
        }

        List<ScalarOperator> scalarOperatorList = Utils.extractConjuncts(logicalExternalOlapScanOperator.getPredicate());
        PartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValueMap,
                columnToNullPartitions, scalarOperatorList, specifyPartitionIds, listPartitionInfo);
        try {
            List<Long> prune = partitionPruner.prune();
            if (prune == null && isTemporaryPartitionPrune) {
                return Lists.newArrayList(partitionIds);
            } else {
                return prune;
            }
        } catch (AnalysisException e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return specifyPartitionIds;

    }

    private static List<Long> rangePartitionPrune(ExternalOlapTable externalOlapTable, RangePartitionInfo rangePartitionInfo,
                                                  LogicalExternalOlapScanOperator logicalExternalOlapScanOperator) {
        Map<Long, Range<PartitionKey>> keyRangeById;
        if (logicalExternalOlapScanOperator.getPartitionNames() != null && logicalExternalOlapScanOperator.getPartitionNames().getPartitionNames() != null) {
            keyRangeById = Maps.newHashMap();
            for (String partName : logicalExternalOlapScanOperator.getPartitionNames().getPartitionNames()) {
                Partition part = externalOlapTable.getPartition(partName, logicalExternalOlapScanOperator.getPartitionNames().isTemp());
                if (part == null) {
                    continue;
                }
                keyRangeById.put(part.getId(), rangePartitionInfo.getRange(part.getId()));
            }
        } else {
            keyRangeById = rangePartitionInfo.getIdToRange(false);
        }
        PartitionPruner partitionPruner = new RangePartitionPruner(keyRangeById,
                rangePartitionInfo.getPartitionColumns(externalOlapTable.getIdToColumn()),
                logicalExternalOlapScanOperator.getColumnFilters());
        try {
            return partitionPruner.prune();
        } catch (Exception e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return Lists.newArrayList(keyRangeById.keySet());
    }



    private static boolean isNeedFurtherPrune(List<Long> candidatePartitions, LogicalExternalOlapScanOperator logicalExternalOlapScanOperator,
                                              PartitionInfo partitionInfo) {
        if (candidatePartitions.isEmpty() ||
        logicalExternalOlapScanOperator.getPredicate() == null) {
            return false;
        }

        // only support RANGE and EXPR_RANGE
        // EXPR_RANGE_V2 type like partition by RANGE(cast(substring(col, 3)) as int)) is unsupported
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            List<Expr> partitionExpr = expressionRangePartitionInfo.getPartitionExprs(logicalExternalOlapScanOperator.getTable().getIdToColumn());
            if (partitionExpr.size() == 1 && partitionExpr.get(0) instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr.get(0);
                String functionName = functionCallExpr.getFnName().getFunction();
                return (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName))
                        || FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName)
                        && !expressionRangePartitionInfo.getIdToRange(true).containsKey(candidatePartitions.get(0));
            }
        } else if (partitionInfo instanceof ExpressionRangePartitionInfoV2) {
            return false;
        } else if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            return rangePartitionInfo.getPartitionColumnsSize() == 1
                    && !rangePartitionInfo.getIdToRange(true).containsKey(candidatePartitions.get(0));
        }
        return false;
    }

    private static boolean containsNullValue(PartitionKey minRange) {
        PartitionKey nullValue = new PartitionKey();
        try {
            for (int i = 0; i < minRange.getKeys().size(); ++i) {
                LiteralExpr rangeKey = minRange.getKeys().get(i);
                PrimitiveType type = minRange.getTypes().get(i);
                nullValue.pushColumn(LiteralExpr.createInfinity(rangeKey.getType(), false), type);
            }
            return minRange.compareTo(nullValue) <= 0;
        } catch (AnalysisException e) {
            return false;
        }

    }

    private static boolean checkFilterNullValue(List<ScalarOperator> scanPredicates, ScalarOperator predicate) {
        ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
        ScalarOperator newPredicate = Utils.compoundAnd(scanPredicates);
        boolean newPredicateFilterNulls = false;
        boolean predicateFilterNulls = false;

        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                return ConstantOperator.createNull(variable.getType());
            }
        };

        if (newPredicate != null) {
            newPredicate = newPredicate.accept(shuttle, null);

            ScalarOperator value = scalarOperatorRewriter.rewrite(newPredicate, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if ((value.isConstantRef() && ((ConstantOperator) value).isNull()) ||
            value.equals(ConstantOperator.createBoolean(false))) {
                newPredicateFilterNulls = true;
            }
        }

        predicate = predicate.accept(shuttle, null);
        ScalarOperator value = scalarOperatorRewriter.rewrite(predicate, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        if ((value.isConstantRef() && ((ConstantOperator) value).isNull())
        || value.equals(ConstantOperator.createBoolean(false))) {
            predicateFilterNulls = true;
        }

        return newPredicateFilterNulls == predicateFilterNulls;
    }


}
