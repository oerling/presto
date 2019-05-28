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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class SubfieldPath
{
    public abstract static class PathElement
    {
        public abstract boolean isSubscript();
    }

    public static final class AllSubscripts
            extends PathElement
    {
        private static final AllSubscripts ALL_SUBSCRIPTS = new AllSubscripts();

        private AllSubscripts() {}

        public static AllSubscripts getInstance()
        {
            return ALL_SUBSCRIPTS;
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "[*]";
        }
    }

    public static final class Cardinality
            extends PathElement
    {
        private static final Cardinality CARDINALITY = new Cardinality();

        private Cardinality() {}

        public static Cardinality getInstance()
        {
            return CARDINALITY;
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "[#]";
        }
    }

    public static final class NestedField
            extends PathElement
    {
        private final String name;

        public NestedField(String name)
        {
            this.name = requireNonNull(name, "name is null");
        }

        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NestedField that = (NestedField) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return "." + name;
        }

        @Override
        public boolean isSubscript()
        {
            return false;
        }
    }

    public static final class LongSubscript
            extends PathElement
    {
        private final long index;
        private final boolean nullIfAbsent;

        public LongSubscript(long index, boolean nullIfAbsent)
        {
            this.index = index;
            this.nullIfAbsent = nullIfAbsent;
        }

        public LongSubscript(long index)
        {
            this(index, false);
        }

        public long getIndex()
        {
            return index;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LongSubscript that = (LongSubscript) o;
            return index == that.index && nullIfAbsent == that.nullIfAbsent;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index);
        }

        @Override
        public String toString()
        {
            return "[" + index + "]";
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }

        public boolean isNullIfAbsent()
        {
            return nullIfAbsent;
        }
    }

    public static final class StringSubscript
            extends PathElement
    {
        private final String index;
        private final boolean nullIfAbsent;

        public StringSubscript(String index, boolean nullIfAbsent)
        {
            this.index = requireNonNull(index, "index is null");
            this.nullIfAbsent = nullIfAbsent;
        }

        public StringSubscript(String index)
        {
            this(index, false);
        }

        public String getIndex()
        {
            return index;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StringSubscript that = (StringSubscript) o;
            return Objects.equals(index, that.index) && nullIfAbsent == that.nullIfAbsent;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index);
        }

        @Override
        public String toString()
        {
            return "[\"" + index.replace("\"", "\\\"") + "\"]";
        }

        @Override
        public boolean isSubscript()
        {
            return true;
        }

        public boolean isNullIfAbsent()
        {
            return nullIfAbsent;
        }
    }

    private final String name;
    private final List<PathElement> path;

    public static PathElement allSubscripts()
    {
        return AllSubscripts.getInstance();
    }

    public static PathElement cardinality()
    {
        return Cardinality.getInstance();
    }

    @JsonCreator
    public SubfieldPath(String path)
    {
        this(Collections.unmodifiableList(parsePath(path)));
    }

    private static List<PathElement> parsePath(String path)
    {
        SubfieldPathTokenizer tokenizer = new SubfieldPathTokenizer(path);
        List<PathElement> elements = new ArrayList<>();
        tokenizer.forEachRemaining(elements::add);
        return elements;
    }

    // TODO Add column name as a separate argument and remove it from the path
    public SubfieldPath(List<PathElement> path)
    {
        requireNonNull(path, "path is null");
        checkArgument(path.size() > 1, "path must include at least 2 elements");
        checkArgument(path.get(0) instanceof NestedField, "path must start with a name");
        this.name = ((NestedField) path.get(0)).getName();
        this.path = path;
    }

    private static void checkArgument(boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public String getColumnName()
    {
        return name;
    }

    public List<PathElement> getPathElements()
    {
        return path;
    }

    public boolean isPrefix(SubfieldPath other)
    {
        if (path.size() < other.path.size()) {
            return Objects.equals(path, other.path.subList(0, path.size()));
        }

        return false;
    }

    @JsonValue
    public String getPath()
    {
        return name + path.subList(1, path.size()).stream()
                .map(PathElement::toString)
                .collect(Collectors.joining());
    }

    @Override
    public String toString()
    {
        return getPath();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SubfieldPath other = (SubfieldPath) o;
        return Objects.equals(path, other.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(path);
    }
}
