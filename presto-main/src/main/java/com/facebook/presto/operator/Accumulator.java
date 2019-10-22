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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;

abstract class Accumulator
{
    int offset;
  Type type;
  abstract void update(byte[][] slabs, int[] offsets, int[] positions, int numPositions, DictionaryBlock[] arguments);
  abstract void initialize(byte[][] slabs, int[] offsets);
  abstract void merge(byte[][] slabs, int[] offsets, int[] positions, Block state);
  abstract void finish(byte[][] slabs, int[] offsets, int numOffsets); 
  abstract int getFixedWidthDataResultSize();
  abstract void getResultSizes(byte[][] slabs, int[] offsets, int numOffsets, int[] lengths);
  abstract Block read(byte[][] slabs, int[] offsets, int numOffsets);
  
}
