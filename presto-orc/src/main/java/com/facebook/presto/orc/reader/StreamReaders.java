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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import org.joda.time.DateTimeZone;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;

public final class StreamReaders
{
    private StreamReaders()
    {
    }
    private static String trace = "";
    private static long traceReadTime;
    private static BufferedWriter traceWriter;

    public static StreamReader createStreamReader(
            StreamDescriptor streamDescriptor,
            DateTimeZone hiveStorageTimeZone,
            AggregatedMemoryContext systemMemoryContext)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case BYTE:
                return new ByteStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongStreamReader(streamDescriptor, systemMemoryContext);
            case FLOAT:
                return new FloatStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case DOUBLE:
                return new DoubleStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceStreamReader(streamDescriptor, systemMemoryContext);
            case TIMESTAMP:
                return new TimestampStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case LIST:
                return new ListStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case STRUCT:
                return new StructStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case MAP:
                return new MapStreamReader(streamDescriptor, hiveStorageTimeZone, systemMemoryContext);
            case DECIMAL:
                return new DecimalStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(StreamReaders.class.getSimpleName()));
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }

    public static void compactArrays(int[] positions, int base, int numPositions, long[] values, boolean[] valueIsNull)
    {
        for (int i = 0; i < numPositions; i++) {
            values[base + i] = values[base + positions[i]];
        }
        compactArray(positions, base, numPositions, valueIsNull);
    }

    public static void compactArrays(int[] positions, int base, int numPositions, int[] values, boolean[] valueIsNull)
    {
        for (int i = 0; i < numPositions; i++) {
            values[base + i] = values[base + positions[i]];
        }
        compactArray(positions, base, numPositions, valueIsNull);
    }

    public static void compactArrays(int[] positions, int base, int numPositions, byte[] values, boolean[] valueIsNull)
    {
        for (int i = 0; i < numPositions; i++) {
            values[base + i] = values[base + positions[i]];
        }
        compactArray(positions, base, numPositions, valueIsNull);
    }

    public static void compactArray(int[] positions, int base, int numPositions, boolean[] values)
    {
        if (values != null) {
            for (int i = 0; i < numPositions; i++) {
                values[base + i] = values[base + positions[i]];
            }
        }
    }

    public static void readTraceSettings()
    {
        long now = System.nanoTime();
        if (now - traceReadTime  < 1000000000) {
            return;
        }
        traceReadTime = now;
        try {
            Path path = Paths.get("/tmp/prestotrace.txt");
            List<String> lines = Files.readAllLines(path);
            StringBuilder builder = new StringBuilder();
            for (String option : lines) {
                builder.append(option);
            }
        trace = builder.toString();
        }
        catch (Exception e) {
            trace = "";
            synchronized(StreamReaders.class) {
                if (traceWriter != null) {
                    try {
                        traceWriter.flush();
                        traceWriter.close();
                        traceWriter = null;
                    }
                    catch (Exception e2) {
                        traceWriter = null;
                    }
                }
            }
        }
    }

    public static boolean isTrace(String pattern)
    {
        return trace.contains(pattern);
    }

    public static void trace(String text)
    {
        synchronized(StreamReaders.class) {
            try {
                if (traceWriter == null) {
                    traceWriter = Files.newBufferedWriter(Paths.get("/tmp/prestoreaders.out"), CREATE);
                }
                traceWriter.append(text);
                traceWriter.newLine();
            }
            catch (Exception e) {
            traceWriter = null;
        }
        }
    }

    public static void flushTrace()
    {
        synchronized (StreamReaders.class) {
            if (traceWriter != null) {
                try {
                    traceWriter.flush();
                }
                catch (Exception e) {
                    traceWriter = null;
                }
            }
        }
    }
}
