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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.SubPlan;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClusterState
{
    private static ExecutorService executor = Executors.newFixedThreadPool(1);

    private static final ClusterState instance = new ClusterState();

    Map<String, NodeState> nodeStates = new HashMap();

    /**
     *
     */
    public static void update(TaskStatus status)
    {
        executor.submit(() -> instance.newStatus(status));
    }

    private void newStatus(TaskStatus status)
    {
        NodeState node = nodeStates.computeIfAbsent(status.getNodeId(), id -> new NodeState(id));
    }

    public void schedule(String queryId, SubPlan subPlan)
    {
    }

    public static class ReservationForecast
    {
        TaskId taskId;
        long initialTime;
        long[] times;
        long[] values;
        int numValues;
        long forecastTime;
        long forecastValue;
        boolean isFinal;

        ReservationForecast(TaskStatus status)
        {
        }

        public long update(TaskStatus status, long now)
        {
            if (numValues == 0) {
                initialTime = now;
            }
            return 0;
        }
    }

    class NodeState
    {
        String nodeId;
        // The sum of the forecast final sizes of everything on this node.
        private long totalReservation;
        private Map<QueryId, QueryState> queryStates = new HashMap();
        NodeState(String nodeId)
        {
            this.nodeId = nodeId;
        }

        long update(TaskStatus status, long now)
        {
            QueryId queryId = status.getTaskId().getQueryId();
            long delta = queryStates.computeIfAbsent(queryId, ignored -> new QueryState(queryId)).update(status, now);
            totalReservation += delta;
            return delta;
        }
    }

    class QueryState
    {
        private QueryId queryId;
        private long totalReservation;
        private Int2ObjectMap<ReservationForecast> forecasts = new Int2ObjectOpenHashMap();

        QueryState(QueryId queryId)
        {
            this.queryId = queryId;
        }

        public long update(TaskStatus status, long now)
        {
            long delta = forecasts.computeIfAbsent(status.getTaskId().getId(), ignored -> new ReservationForecast(status)).update(status, now);
            totalReservation += delta;
            return delta;
        }
    }
}
