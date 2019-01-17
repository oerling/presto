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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.Utils;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.ReferencePath;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.ArrayList;

import static java.util.Collections.reverse;

public class SubfieldUtils
{
    public static boolean isSubfieldPath(Node expression)
    {
        return expression instanceof DereferenceExpression || expression instanceof SubscriptExpression;
    }

    public static ReferencePath subfieldToReferencePath(Node expr)
    {
        ArrayList<ReferencePath.PathElement> steps = new ArrayList();
        for (;;) {
            if (expr instanceof SymbolReference) {
                SymbolReference symbolReference = (SymbolReference)expr;
                steps.add(new ReferencePath.PathElement(symbolReference.getName(), 0));
                break;
            }
            else if (expr instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expr;
                steps.add(new ReferencePath.PathElement(dereference.getField().getValue(), 0));
                expr = dereference.getBase();
            }
            else if (expr instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expr;
                Expression index = subscript.getIndex();
                if (!(index instanceof GenericLiteral)) {
                    return null;
                }
                GenericLiteral literalIndex = (GenericLiteral) index;
                String type = literalIndex.getType();
                if (type.equals("BIGINT")) {
                    steps.add(new ReferencePath.PathElement(null, new Integer(literalIndex.getValue()).intValue(), true));
                }
                else if (type.equals("VARCHAR")) {
                    steps.add(new ReferencePath.PathElement(literalIndex.getValue().toString(), 0, true));
                }
                else {
                    return null;
                }
                expr = subscript.getBase();
            }
            else {
                return null;
            }
        }
        reverse(steps);
        return new ReferencePath(steps);
    }

    public static Node getSubfieldBase(Node expr)
    {
        for (;;) {
            if (expr instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expr;
                expr = dereference.getBase();
            }
            else if (expr instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expr;
                expr = subscript.getBase();
            }
            else {
                return expr;
            }
        }
    }
}

