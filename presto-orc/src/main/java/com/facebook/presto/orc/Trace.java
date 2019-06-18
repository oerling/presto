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
package com.facebook.presto.orc;

import java.io.FileWriter;
import java.io.IOException;

public class Trace
{
    private Trace() {}
    public static final int SEEK = 1;
    public static final int REORDER = 2;

    static int events;
    static boolean isOn;
    static FileWriter writer;

    static void setActive(boolean on)
    {
        isOn = on;
    }

    public static void trace(int event, String str)
    {
        if (!isOn && event != -1) {
            return;
        }
        synchronized (Trace.class) {
            try {
                if (writer == null) {
                    writer = new FileWriter("/tmp/trace.out");
                }
                writer.write(str);
                writer.flush();
            }
            catch (IOException e) {
                return;
            }
        }
    }
}
