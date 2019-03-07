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

import java.util.ArrayList;

public interface ColumnHandle
{
    default boolean supportsSubfieldPruning()
    {
        return false;
    }

    default boolean supportsSubfieldTupleDomain()
    {
        return false;
    }

    /* Returns a ColumnHandle that refers to the subfield at
     * 'path'. Such a ColumnHandle may occur as a key in a TupleDomain
     * for filtering non-top level columns */
    default ColumnHandle createSubfieldColumnHandle(SubfieldPath path)
    {
        throw new UnsupportedOperationException();
    }

    /* Returns an equivalent ColumnHandle where the connector is free
     * to leave out any subfields not in the 'paths'. Such a ColumnHandle may occur in the list of projected columns for a PageSource.  */
    default ColumnHandle createSubfieldPruningColumnHandle(ArrayList<SubfieldPath> referencedSubfields)
    {
        return this;
    }

    default SubfieldPath getSubfieldPath()
    {
        return null;
    }

    default ArrayList<SubfieldPath> getReferencedSubfields()
    {
        return null;
    }
}
