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

import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class SubfieldUtils
{
    private SubfieldUtils() {}

    public static boolean isDereferenceOrSubscriptExpression(Node expression)
    {
        return expression instanceof DereferenceExpression || expression instanceof SubscriptExpression;
    }

    public static SubfieldPath deferenceOrSubscriptExpressionToPath(Node expression)
    {
        ImmutableList.Builder<SubfieldPath.PathElement> elements = ImmutableList.builder();
        while (true) {
            if (expression instanceof SymbolReference) {
                elements.add(new SubfieldPath.NestedField(((SymbolReference) expression).getName()));
                return new SubfieldPath(elements.build().reverse());
            }

            if (expression instanceof DereferenceExpression) {
                DereferenceExpression dereference = (DereferenceExpression) expression;
                elements.add(new SubfieldPath.NestedField(dereference.getField().getValue()));
                expression = dereference.getBase();
            }
            else if (expression instanceof SubscriptExpression) {
                SubscriptExpression subscript = (SubscriptExpression) expression;
                Expression index = subscript.getIndex();
                if (index instanceof Cast) {
                    // map['str'] can be represented as a
                    // SubscriptExpression with an index of cast of a
                    // string literal to a varchar(xx).
                    Cast cast = (Cast) index;
                    index = cast.getExpression();
                    if (!(index instanceof StringLiteral) || !cast.getType().startsWith("varchar")) {
                        return null;
                    }
                }
                if (index instanceof LongLiteral) {
                    elements.add(new SubfieldPath.LongSubscript(((LongLiteral) index).getValue()));
                }
                else if (index instanceof StringLiteral) {
                    elements.add(new SubfieldPath.StringSubscript(((StringLiteral) index).getValue()));
                }
                else if (index instanceof GenericLiteral) {
                    GenericLiteral literal = (GenericLiteral) index;
                    if (BIGINT.getTypeSignature().equals(TypeSignature.parseTypeSignature(literal.getType()))) {
                        elements.add(new SubfieldPath.LongSubscript(Long.valueOf(literal.getValue())));
                    }
                    else {
                        return null;
                    }
                }
                else {
                    return null;
                }
                expression = subscript.getBase();
            }
            else {
                return null;
            }
        }
    }

    public static Expression getDerefenceOrSubscriptBase(Expression expression)
    {
        while (true) {
            if (expression instanceof DereferenceExpression) {
                expression = ((DereferenceExpression) expression).getBase();
            }
            else if (expression instanceof SubscriptExpression) {
                expression = ((SubscriptExpression) expression).getBase();
            }
            else {
                return expression;
            }
        }
    }

    public static Type getType(SubfieldPath path, TypeProvider types)
    {
        List<SubfieldPath.PathElement> elements = path.getPathElements();
        SubfieldPath.NestedField head = (SubfieldPath.NestedField) elements.get(0);
        Type type = types.get(new Symbol(head.getName()));
        for (int i = 1; i < elements.size(); i++) {
            SubfieldPath.PathElement element = elements.get(i);
            if (element instanceof SubfieldPath.LongSubscript || element instanceof SubfieldPath.StringSubscript || element instanceof SubfieldPath.AllSubscripts) {
                if (type instanceof MapType) {
                    type = type.getTypeParameters().get(1);
                }
                else {
                    type = type.getTypeParameters().get(0);
                }
            }
            else {
                List<RowType.Field> fields = ((RowType) type).getFields();
                SubfieldPath.NestedField nestedField = (SubfieldPath.NestedField) element;
                boolean found = false;
                String referenceName = nestedField.getName().toLowerCase(Locale.ENGLISH);
                for (RowType.Field field : fields) {
                    Optional<String> name = field.getName();
                    if (!name.isPresent()) {
                        continue;
                    }
                    if (name.get().toLowerCase(Locale.ENGLISH).equals(referenceName)) {
                        type = field.getType();
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("No field " + nestedField.getName());
                }
            }
        }
        return type;
}
}
