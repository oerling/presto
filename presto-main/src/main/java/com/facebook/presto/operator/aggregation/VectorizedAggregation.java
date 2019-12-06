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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.HashCollisionsCounter;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.SimpleArrayAllocator;
import com.facebook.presto.operator.TransformWork;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.operator.WorkProcessor.ProcessState;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.operator.aggregation.VectorizedHashTable;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.ArrayAllocator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class VectorizedAggregation
{
    private final VectorizedHashTable groupByHash;
    private final List<Aggregator> aggregators;
    private final OperatorContext operatorContext;
    private final boolean partial;
    private final OptionalLong maxPartialMemory;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final boolean useSystemMemory;

    private boolean full;

        private final ArrayAllocator arrayAllocator = new SimpleArrayAllocator(50);
        private final BlockFlattener flattener = new BlockFlattener(arrayAllocator);
        private final Closer blockLeaseCloser = Closer.create();
    Block[] blocks;
    
    public VectorizedAggregation(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            boolean yieldForMemoryReservation,
            boolean useSystemMemory)
    {
        this(accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                maxPartialMemory,
                Optional.empty(),
                joinCompiler,
                yieldForMemoryReservation,
                useSystemMemory);
    }

    public VectorizedAggregation(
            List<AccumulatorFactory> accumulatorFactories,
            Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            boolean yieldForMemoryReservation,
            boolean useSystemMemory)
    {
        this.operatorContext = operatorContext;
        this.partial = step.isOutputPartial();
        this.maxPartialMemory = maxPartialMemory.map(dataSize -> OptionalLong.of(dataSize.toBytes())).orElseGet(OptionalLong::empty);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(VectorizedAggregation.class.getSimpleName());
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.useSystemMemory = useSystemMemory;

        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        for (int i = 0; i < accumulatorFactories.size(); i++) {
            AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
            Optional<Integer> overwriteIntermediateChannel = Optional.empty();
            if (overwriteIntermediateChannelOffset.isPresent()) {
                overwriteIntermediateChannel = Optional.of(overwriteIntermediateChannelOffset.get() + i);
            }
            builder.add(new Aggregator(accumulatorFactory, step, overwriteIntermediateChannel));
        }
        aggregators = builder.build();
        this.groupByHash = new VectorizedHashTable(
                groupByTypes,
                ImmutableList.of(),
                Ints.toArray(groupByChannels),
                hashChannel,
                new int[0],
                aggregators,
                true,
                expectedGroups);

    }

    public void close()
    {
        groupByHash.close();
        updateMemory(0);

    }

    public void addInput(Page page)
    {
        if (blocks == null) {
            blocks = new Block[page.getChannelCount()];

        }
        for (int i = 0; i < page.getChannelCount(); i++) {
            BlockLease lease = flattener.flatten(page.getBlock(i));
            blockLeaseCloser.register(lease::close);
            blocks[i] = lease.get();
        }
        groupByHash.groupByInput(blocks);
        try {
            blockLeaseCloser.close();
        }
        catch (Exception e) {
            verify(false, "BlockLease is not supposed to throw");
        }
    }

    public boolean isFull()
    {
        return full;
    }

    public long getSizeInMemory()
    {
        long sizeInMemory = groupByHash.getEstimatedSize();
        return sizeInMemory;
    }

    public int getKeyChannels()
    {
        return groupByHash.getTypes().size();
    }

    public long getGroupCount()
    {
        return groupByHash.getGroupCount();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }

    public Page getOutput()
    {
        return null;
    }

    public List<Type> buildTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupByHash.getTypes());
        for (Aggregator aggregator : aggregators) {
            types.add(aggregator.getType());
        }
        return types;
    }

    private void updateMemory(long memorySize)
    {
        if (useSystemMemory) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            localUserMemoryContext.setBytes(memorySize);
        }
    }

    public static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private AggregationNode.Step step;
        private final int intermediateChannel;
        private final String name;
        private final int[] inputChannels;

        private Aggregator(AccumulatorFactory accumulatorFactory, AggregationNode.Step step, Optional<Integer> overwriteIntermediateChannel)
        {
            if (step.isInputRaw()) {
                this.intermediateChannel = -1;
                this.aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else if (overwriteIntermediateChannel.isPresent()) {
                this.intermediateChannel = overwriteIntermediateChannel.get();
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                this.intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
            this.name = accumulatorFactory.getName();
            this.inputChannels = Ints.toArray(accumulatorFactory.getInputChannels());
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public String getName()
        {
            return name;
        }

        public int[] getInputChannels()
        {
            return inputChannels;
        }
        
        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
                aggregation.addInput(groupIds, page);
            }
            else {
                aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
            }
        }

        public void prepareFinal()
        {
            aggregation.prepareFinal();
        }

        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
            }
        }

        public void setOutputPartial()
        {
            step = AggregationNode.Step.partialOutput(step);
        }

        public Type getIntermediateType()
        {
            return aggregation.getIntermediateType();
        }
    }

    public static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new Aggregator(factory, step, Optional.empty()).getType());
        }
        return types.build();
    }
}



