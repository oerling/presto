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

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;

public class Trace
{
    private static String trace = "";
    private static long traceReadTime;
    private static BufferedWriter traceWriter;
    private static long openTimeMillis;
    private static boolean printTime;

    private Trace() {}

    public static void readTraceSettings()
    {
        long now = System.nanoTime();
        if (now - traceReadTime < 1000000000) {
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

    public static boolean isTrace(String pattern)
    {
        return trace.contains(pattern);
    }

    public static void trace(String text)
    {
        synchronized (Trace.class) {
            try {
                if (traceWriter == null) {
                    traceWriter = Files.newBufferedWriter(Paths.get("/tmp/presto.out"), CREATE);
                    if (printTime) {
                        traceWriter.append("Trace");
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
}
