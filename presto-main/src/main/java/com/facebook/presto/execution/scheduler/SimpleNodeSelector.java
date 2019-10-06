/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.recordSplitAssignment;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SimpleNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SimpleNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final boolean useAffinity;

    public SimpleNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
                              boolean useAffinity)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.useAffinity = useAffinity;
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);
        ResettableRandomizedIterator<InternalNode> randomCandidates = randomizedNodes(nodeMap, includeCoordinator, ImmutableSet.of());
        List<InternalNode> nodesForAffinity = null;
        if (useAffinity) {
            nodesForAffinity = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .collect(toImmutableList());
        }
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;
        for (Split split : splits) {
            randomCandidates.reset();

            List<InternalNode> candidateNodes;
            if (!split.isRemotelyAccessible()) {
                candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);
            }
            else if (useAffinity) {
                candidateNodes = selectAffinityNodesForSplit(nodesForAffinity, split, minCandidates, randomCandidates);
            }
                else {
                candidateNodes = selectNodes(minCandidates, randomCandidates);
            }
            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            InternalNode chosenNode = null;
            int min = Integer.MAX_VALUE;

            if (!useAffinity) {
                recordSplitAssignment(false, false);
            }
            for (InternalNode node : candidateNodes) {
                int totalSplitCount = assignmentStats.getTotalSplitCount(node);
                if (totalSplitCount < min && totalSplitCount < maxSplitsPerNode) {
                    chosenNode = node;
                    min = totalSplitCount;
                }
            }
            if (chosenNode == null) {
                // min is guaranteed to be MAX_VALUE at this line
                if (useAffinity) {
                    // The preferred node is busy, revert to random.
                    recordSplitAssignment(true, true);
                    candidateNodes = selectNodes(minCandidates, randomCandidates);
                }
                for (InternalNode node : candidateNodes) {
                    int totalSplitCount = assignmentStats.getQueuedSplitCountForStage(node);
                    if (totalSplitCount < min && totalSplitCount < maxPendingSplitsPerTask) {
                        chosenNode = node;
                        min = totalSplitCount;
                    }
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
                if (useAffinity) {
                    recordSplitAssignment(true, false);
                }
            }
            else {
                if (split.isRemotelyAccessible()) {
                    splitWaitingForAnyNode = true;
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
            }
        }

        ListenableFuture<?> blocked;
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    List<InternalNode> selectAffinityNodesForSplit(List<InternalNode> workers, Split split, int limit, ResettableRandomizedIterator<InternalNode> candidates)
    {
        Object info = split.getInfo();
        // If we understand the info, we use a hash of file name, else we random nodes.
        if (info instanceof Map) {
            Object path = ((Map) info).get("path");
            if (path instanceof String) {
                int hash = ((String) path).hashCode();
                return ImmutableList.of(workers.get((hash & 0xffffff) % workers.size()));
            }
        }
        return selectNodes(limit, candidates);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }
}
