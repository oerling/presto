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

import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;

import java.io.IOException;

public abstract class VariantStreamReader
        implements StreamReader
{
    protected StreamReader currentReader;

    // Readers may alternate between variants, e.g. direct vs
    // dictionary. When the change takes place,
    // mustRetrieveResultFromPreviousReader is set. This is tested
    // before beginning with a new row
    // group. resultFromPreviousReaderRetrieved is set when the rows
    // are erased. The next scan finally clears both flags. Note that
    // there can be a reader that accumulates values but is not
    // projected out. These do not fetch the values when returning a
    // Page but are still erased at the start of the next batch.
    private StreamReader previousReader;
    private boolean mustRetrieveResultFromPreviousReader;
    private boolean resultFromPreviousReaderRetrieved;
    protected int columnIndex;
    
    protected void readerChanged(StreamReader previousReader)
    {
        this.previousReader = previousReader;
        if (previousReader != null && previousReader != currentReader && currentReader.getChannel() != -1 && previousReader.getNumValues() > 0) {
            mustRetrieveResultFromPreviousReader = true;
        }
    }

    @Override
    public final boolean mustExtractValuesBeforeScan(boolean isNewStripe)
    {
        if (mustRetrieveResultFromPreviousReader) {
            return true;
        }
        return currentReader.mustExtractValuesBeforeScan(isNewStripe);
    }

    @Override
    public final Block getBlock(int numFirstRows, boolean mayReuse)
    {
        if (mustRetrieveResultFromPreviousReader) {
            return previousReader.getBlock(numFirstRows, mayReuse);
        }
        return currentReader.getBlock(numFirstRows, mayReuse);
    }

    public final int getColumnIndex()
    {
        return columnIndex;
    }
    
    @Override
    public final void scan()
            throws IOException
    {
        if (mustRetrieveResultFromPreviousReader && !resultFromPreviousReaderRetrieved) {
            throw new UnsupportedOperationException("Must retrieve result from a previous reader before starting with the next, previous = " + previousReader.toString() + " current = " + currentReader.toString());
        }
        mustRetrieveResultFromPreviousReader = false;
        resultFromPreviousReaderRetrieved = false;
        currentReader.scan();
    }

    @Override
    public final void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public final void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        currentReader.setInputQualifyingSet(qualifyingSet);
    }

    @Override
    public final QualifyingSet getInputQualifyingSet()
    {
        return currentReader.getInputQualifyingSet();
    }

    @Override
    public final QualifyingSet getOutputQualifyingSet()
    {
        return currentReader.getOutputQualifyingSet();
    }

    @Override
    public final void setOutputQualifyingSet(QualifyingSet set)
    {
        currentReader.setOutputQualifyingSet(set);
    }

    @Override
    public final QualifyingSet getOrCreateOutputQualifyingSet()
    {
        return currentReader.getOrCreateOutputQualifyingSet();
    }

    @Override
    public final void erase(int end)
    {
        if (currentReader == null) {
            return;
        }
        if (mustRetrieveResultFromPreviousReader) {
            resultFromPreviousReaderRetrieved = true;
            previousReader.erase(end);
        }
        else {
            currentReader.erase(end);
        }
    }

    @Override
    public final void compactValues(int[] positions, int base, int numPositions)
    {
        currentReader.compactValues(positions, base, numPositions);
    }

    @Override
    public final int getPosition()
    {
        return currentReader.getPosition();
    }

    @Override
    public final int getResultSizeInBytes()
    {
        if (currentReader == null) {
            return 0;
        }
        return currentReader.getResultSizeInBytes();
    }

    @Override
    public final int getNumValues()
    {
        return currentReader.getNumValues();
    }

    @Override
    public final void setResultSizeBudget(long bytes)
    {
        currentReader.setResultSizeBudget(bytes);
    }
}
