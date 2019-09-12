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

import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ClusterState
{

    private static executor = Executors.newFixedThreadPool(1);

    private static final ClusterState instance = new ClusterState();

    Map<InternalNode, NodeState> nodeStates = new HashMap();
    
    /**
     * 
     */
    public static void update(TaskStatus state)
    {
        executor.submit(() -> newStatus(status));
    }

    private void newStatus(TaskStatus status)
    {
    }

    public List<InternalNode> assignSplitLocations(List<Split>)
    {
        return null;
    }

    public void schedule(String queryId, List<StageShape> stages)
    {
    }

    public static History
    {
        long initialTime;
        long[] times;
        long[] values;
        int numValues;
        long forecastTime;
        long forecastValue;
        boolean isFinal;
    }

    class NodeState
    {
        // The sum of the final sizes of everything on this node.
        long totalReservation;
        Map<String, QueryState> queryStates;
    }

    class QueryState
    {
        long totalReservation;
        Map<String, TaskReservation> taskStates;
    }

    class TaskReservation
    {
        History history;
    }


}
