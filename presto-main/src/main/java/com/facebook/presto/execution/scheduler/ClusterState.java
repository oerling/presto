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
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.trace.Trace;
import com.facebook.presto.sql.planner.SubPlan;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;

    import java.util.Arrays;
    import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Float.isNaN;

public class ClusterState
{
    private static ExecutorService executor = Executors.newFixedThreadPool(1);

    private static final ClusterState instance = new ClusterState();

    private Map<String, NodeState> nodeStates = new HashMap();
    private Map<QueryId, Set<NodeState>> queryToNodes = new HashMap();
    private long lastTraceTime;

    private static Map<String, Integer> shortNames = new HashMap();
    private static int nameCounter;


    /**
     *
     */
    public static void update(TaskStatus status)
    {
        //executor.submit(() -> instance.newStatus(status));
    }

    public static void queryFinished(QueryId queryId)
    {
        //executor.submit(() -> instance.freeQuery(queryId));
    }
    
    private void newStatus(TaskStatus status)
    {
        QueryId queryId = status.getTaskId().getQueryId();
        NodeState node = nodeStates.computeIfAbsent(status.getNodeId(), id -> new NodeState(id));
        node.update(status, System.nanoTime());
        queryToNodes.computeIfAbsent(queryId, ignore -> new HashSet()).add(node);
        trace();
    }

    private void freeQuery(QueryId queryId)
    {
        Set<NodeState> nodes = queryToNodes.get(queryId);
        if (nodes == null) {
            return;
        }
        for (NodeState node : nodes) {
            node.freeQuery(queryId);
        }
        trace();
    }

    public void schedule(String queryId, SubPlan subPlan)
    {
    }

    public String report()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("--- Cluster Resources\n");
        NodeState[] nodes = nodeStates.values().stream().toArray(size -> new NodeState[size]);
        Arrays.sort(nodes, (NodeState left, NodeState right) -> left.reservation.actualMemory < right.reservation.actualMemory ? -1 : left.reservation.actualMemory == right.reservation.actualMemory ? 0 : 1 );
        for (NodeState node : nodes) {
            builder.append(node.toString());
            builder.append("\n");
        }
        return builder.toString();
    }

    private void trace()
    {
        if (Trace.isTrace("clusterstate")) {
            long now = System.nanoTime();
            if (now - lastTraceTime > 2000000000) {
                Trace.trace(report());
                lastTraceTime = System.nanoTime();
            }
        }
    }
    
    public static class Reservation
    {
        long actualMemory;
        long expectedMemory;
        // Number of threads scheduled in last reporting interval
        float threadsScheduled = 0;
        // Number of threads on CPU in last reporting interval
        float threadsOnCpu = 0;

        public Reservation(long actualMemory, long expectedMemory)
        {
            this.actualMemory = actualMemory;
            this.expectedMemory = expectedMemory;
        }

        public Reservation(long actualMemory, long expectedMemory, float threadsScheduled, float threadsOnCpu)
        {
            this.actualMemory = actualMemory;
            this.expectedMemory = expectedMemory;
            this.threadsScheduled = threadsScheduled;
            this.threadsOnCpu = threadsOnCpu;
        }

        public void increment(Reservation other)
        {
            actualMemory += other.actualMemory;
            expectedMemory += other.expectedMemory;
            threadsScheduled += other.threadsScheduled;
            threadsOnCpu  += other.threadsOnCpu;
        }

        public void decrement(Reservation other)
        {
            actualMemory -= other.actualMemory;
            expectedMemory -= other.expectedMemory;
            threadsScheduled -= other.threadsScheduled;
            threadsOnCpu  -= other.threadsOnCpu;
        }

        @Override
        public String toString()
        {
            return "<Reservation " + actualMemory + "/" + expectedMemory + " threads: " + threadsOnCpu + "/" + threadsScheduled + ">";
        }
    }

    public static class ReservationForecast
    {
        TaskId taskId;
        long initialTime;
        LongArrayList times = new LongArrayList();
        LongArrayList values = new LongArrayList();
        LongArrayList scheduledTime = new LongArrayList();
        LongArrayList cpuTime = new LongArrayList();
        float previousThreadsScheduled = 0;
        float previousThreadsOnCpu = 0;
        long forecastTime;
        Reservation reservation = new Reservation(0, 0);
        boolean isFinal;

        ReservationForecast ()
        {
        }
        
        ReservationForecast(TaskStatus status)
        {
        }

        public Reservation update(TaskStatus status, long now)
        {
            if (values.size() == 0) {
                initialTime = now;
            }
            long mem = status.getSystemMemoryReservation().toBytes() + status.getMemoryReservation().toBytes();
            long previousMem = values.size() == 0 ? 0 : values.get(values.size() - 1);
            values.add(mem);
            times.add(now);
            scheduledTime.add(status.getScheduledTime().toMillis());
            cpuTime.add(status.getCpuTime().toMillis());
            int numPoints = scheduledTime.size();
            if (numPoints > 1 && (cpuTime.get(numPoints - 1) < cpuTime.get(numPoints - 2))) {
                System.out.println("***negative cpu time");
            }
            long interval = numPoints == 1 ? 1 : Math.max(1, (times.get(numPoints - 1) - times.get(numPoints - 2)) / 1000000);
            float threadsScheduled = numPoints == 1 ? 1 : ((float)scheduledTime.get(numPoints - 1) - scheduledTime.get(numPoints - 2)) / interval;
            float threadsOnCpu = numPoints == 1 ? 1 : ((float)cpuTime.get(numPoints - 1) - cpuTime.get(numPoints - 2)) / interval;
            reservation.actualMemory = mem;
            if (isNaN(threadsOnCpu)) {
                System.out.println("***Nan");
            }
            Reservation result = new Reservation(mem - previousMem, 0, threadsScheduled - previousThreadsScheduled, threadsOnCpu - previousThreadsOnCpu);
            reservation.threadsScheduled = threadsScheduled;
            reservation.threadsOnCpu = threadsOnCpu;
            previousThreadsScheduled = threadsScheduled;
                previousThreadsOnCpu = threadsOnCpu;
                return result;
        }
    }

    class NodeState
    {
        String nodeId;
        // The sum of the forecast final sizes of everything on this node.
        private Reservation reservation = new Reservation(0, 0);
        private Map<QueryId, QueryState> queryStates = new HashMap();

        NodeState(String nodeId)
        {
            this.nodeId = nodeId;
        }

        Reservation update(TaskStatus status, long now)
        {
            QueryId queryId = status.getTaskId().getQueryId();
            Reservation delta = queryStates.computeIfAbsent(queryId, ignored -> new QueryState(queryId)).update(status, now);
            reservation.increment(delta);
            return delta;
        }

        void freeQuery(QueryId queryId)
        {
            QueryState state = queryStates.get(queryId);
            if (state == null) {
                return;
            }
            reservation.decrement(state.free());
            queryStates.remove(queryId);
        }

        private String shortName()
        {
            Integer n = shortNames.computeIfAbsent(nodeId, ignore -> Integer.valueOf(++nameCounter));
            return "N" + n;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            ;
            builder.append("<Node ");
            builder.append(shortName());
            builder.append(reservation.toString());
            builder.append(" " + queryStates.size() + "qs>");
            return builder.toString();
        }
    }

    class QueryState
    {
        private QueryId queryId;
        private Reservation reservation = new Reservation(0, 0);
        private Int2ObjectMap<ReservationForecast> forecasts = new Int2ObjectOpenHashMap();

        QueryState(QueryId queryId)
        {
            this.queryId = queryId;
        }

        public Reservation update(TaskStatus status, long now)
        {
            Reservation delta = forecasts.computeIfAbsent(status.getTaskId().getId(), ignored -> new ReservationForecast()).update(status, now);
            reservation.increment(delta);
            return delta;
        }

        Reservation free()
        {
            return reservation;
        }
    }
}
