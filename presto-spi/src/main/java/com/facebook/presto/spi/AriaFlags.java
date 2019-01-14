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
package com.facebook.presto.spi;

public class AriaFlags
{
    public static final int repartition = 1;
    public static final int hashJoin = 2;
    public static final int orcBufferReuse = 4;
    public static final int exchangeReuse = 8;
    public static final int noReaderBudget = 16;
    public static final int exchangeReusePages = 32;
    public static final int pruneSubfields = 64;
}
