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
package com.facebook.presto.spi.trace;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import io.airlift.slice.Slice;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.CREATE;

public class Trace
{
    private static String trace = "";
    private static long traceReadTime;
    private static BufferedWriter traceWriter;
    private static long openTimeMillis;
    private static boolean printTime;
    private static Map<Integer, Integer> proxyPorts = new HashMap();

    private Trace() {}

    public static void readTraceSettings()
    {
        long now = System.nanoTime();
        if (now - traceReadTime < 1000000000) {
            return;
        }
        traceReadTime = now;
        checkDeleteTrace();
        try {
            Path path = Paths.get("/tmp/prestotrace.txt");
            List<String> lines = Files.readAllLines(path);
            StringBuilder builder = new StringBuilder();
            for (String option : lines) {
                builder.append(option);
                builder.append(" ");
            }
            trace = builder.toString();
            printTime = trace.contains("time");
        }
        catch (Exception e) {
            trace = "";
            synchronized (Trace.class) {
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

    private static void checkDeleteTrace()
    {
        try {
            Path path = Paths.get("/tmp/deletetrace.txt");
            List<String> lines = Files.readAllLines(path);
            synchronized (Trace.class) {
                Files.delete(path);
                if (traceWriter != null) {
                    try {
                        traceWriter.flush();
                        traceWriter.close();
                        traceWriter = null;
                        Files.delete(Paths.get("/tmp/prestotrace.txt"));
                    }
                    catch (Exception e2) {
                        traceWriter = null;
                    }
                }
            }
        }
        catch (Exception e) {
        }
    }

    public static boolean isTrace(String pattern)
    {
        readTraceSettings();
        return trace.contains(pattern);
    }

    public static void trace(String text)
    {
        synchronized (Trace.class) {
            try {
                if (traceWriter == null) {
                    traceWriter = Files.newBufferedWriter(Paths.get("/tmp/presto.out"), CREATE);
                    if (printTime) {
                        traceWriter.append("Trace: ");
                        traceWriter.append(trace);
                        traceWriter.newLine();
                    }
                    openTimeMillis = milliTime();
                }
                if (printTime) {
                    traceWriter.append("T+");
                    traceWriter.append(Long.valueOf(milliTime() - openTimeMillis).toString());
                    traceWriter.append(" ");
                }
                traceWriter.append(text);
                traceWriter.newLine();
            }
            catch (Exception e) {
                traceWriter = null;
            }
        }
        flushTrace();
    }

    public static void flushTrace()
    {
        synchronized (Trace.class) {
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

    private static long milliTime()
    {
        return System.nanoTime() / 1000000;
    }

    public static String stackTrace(int maxFrames)
    {
        StringBuilder result = new StringBuilder();
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (int i = 1; i < elements.length && i < maxFrames; i++) {
            StackTraceElement s = elements[i];
            result.append(s.getClassName() + "." + s.getMethodName() + "(" + s.getFileName() + ":" + s.getLineNumber() + ")\n");
        }
        return result.toString();
    }

    public static void tracePage(String message, Page page)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(message);
        builder.append("\n Page" + page.getPositionCount() + " rows:\n");
        for (int i = 0; i < page.getPositionCount(); i++) {
            builder.append("Row " + i + ": ");
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                if (block instanceof LongArrayBlock) {
                    builder.append(Long.valueOf(((LongArrayBlock) block).getLong(i)).toString());
                }
                else if (block instanceof VariableWidthBlock) {
                    VariableWidthBlock values = (VariableWidthBlock) block;
                    Slice slice = values.getSlice(i, 0, values.getSliceLength(i));
                    builder.append(slice.toStringUtf8());
                }
                else {
                    builder.append("<***>");
                }
                builder.append(", ");
            }
            builder.append("\n");
        }
        trace(builder.toString());
        flushTrace();
    }

    public static String setProxyPort(String url)
    {
        int i = url.lastIndexOf(":");
        if (i < 0) {
            throw new IllegalArgumentException("url has no port when setting proxy: " + url);
        }
        String command = "getproxyport.sh " + url.substring(i + 1);
        String line;
        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            line = reader.readLine();
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Could not read tracing port");
        }
        int otherPort = Integer.valueOf(line);
        int originPort = Integer.valueOf(url.substring(i + 1));
        proxyPorts.put(originPort, otherPort);
        return url.substring(0, i + 1) + String.valueOf(otherPort);
    }

    public static String setProxyPortIfExists(String url)
    {
        if (proxyPorts.size() == 0) {
            return url;
        }
        int i = url.lastIndexOf(":");
        if (i < 0) {
            throw new IllegalArgumentException("url has no port when setting proxy: " + url);
        }
        int originPort = Integer.valueOf(url.substring(i + 1));
        Integer port = proxyPorts.get(originPort);
        if (port == null) {
            return url;
        }
        return url.substring(0, i + 1) + String.valueOf(port);
    }
}
