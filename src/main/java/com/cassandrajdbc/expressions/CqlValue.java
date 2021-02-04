/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.cassandrajdbc.util.MathUtil;
import com.google.common.base.Objects;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class CqlValue {
    
    public static final String PARAM = "?param?";

    public static List<Object[]> toCqlValueList(ItemsList itemList) {
        ValueListExtractor extractor = new ValueListExtractor();
        itemList.accept(extractor);
        return extractor.getValues();
    }

    public static Object toCqlValue(Expression expression) {
        ValueExtractor extractor = new ValueExtractor();
        expression.accept(extractor);
        return extractor.getValue();
    }
    
    
    private static class ValueListExtractor implements ItemsListVisitor {

        private List<Object[]> values;

        @Override
        public void visit(SubSelect subSelect) {
            throw new UnsupportedOperationException("Subselects are not supported");
        }

        @Override
        public void visit(ExpressionList expressionList) {
            values = Arrays.<Object[]>asList(extract(expressionList));
        }

        @Override
        public void visit(MultiExpressionList multiExprList) {
            values = multiExprList.getExprList().stream()
                .map(this::extract)
                .collect(Collectors.toList());
        }

        private Object[] extract(ExpressionList expressionList) {
            return expressionList.getExpressions().stream()
                .map(CqlValue::toCqlValue)
                .toArray();
        }
        
        public List<Object[]> getValues() {
            return values;
        }
        
    }
    
    private static class ValueExtractor extends ExpressionVisitorAdapter {
        
        private Object value;

        @Override
        public void visit(NullValue nullValue) {
            value = null;
        }

        @Override
        public void visit(StringValue stringValue) {
            value = stringValue.getValue();
            if(stringValue.getValue().matches("\\d{4}-\\d{2}-\\d{2}")) {
                value = java.time.LocalDate.parse(stringValue.getValue())
                    .minusMonths(1)
                    .format(java.time.format.DateTimeFormatter.ISO_DATE); // new java.sql.Date constructor month starting with 0 fix (inconsistency between string->date and date->date inserts) 
            } else {
                value = stringValue.getValue();
            }
        }
        
        @Override
        public void visit(DoubleValue doubleValue) {
            value = doubleValue.getValue();
        }

        @Override
        public void visit(LongValue longValue) {
            value = longValue.getValue();
        }

        @Override
        public void visit(DateValue dateValue) {
            value = convert(dateValue.getValue(), v -> new Date(v.getTime()));
        }

        @Override
        public void visit(TimestampValue timestampValue) {
            value = convert(timestampValue.getValue(), v -> new Date(v.getTime()));
        }
        
        @Override
        public void visit(TimeValue timeValue) {
            value = convert(timeValue.getValue(), v -> v.getTime());
        }

        @Override
        public void visit(HexValue hexValue) {
            value = hexValue.getValue();
        }
        
        @Override
        public void visit(Column column) {
            if("TRUE".equals(column.getColumnName())) {
                value = true;
            } else if("FALSE".equals(column.getColumnName())) {
                value = false;
            } else {
                unknown(column);
            }
        }

        @Override
        public void visit(JdbcParameter param) {
            value = PARAM;
        }

        @Override
        public void visit(DateTimeLiteralExpression literal) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(JsonExpression jsonExpr) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(TimeKeyExpression timeKeyExpression) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(Function function) {
            unknown(function);
        }
        @Override
        public void visit(SignedExpression signedExpression) {
            unknown(signedExpression);
        }

        @Override
        public void visit(Parenthesis parenthesis) {
            parenthesis.getExpression().accept(this);
        }

        @Override
        public void visit(Addition addition) {
            Object[] vals = extractValues(addition);
            if(vals[0] instanceof String || vals[1] instanceof String) {
                value = String.valueOf(vals[0]) + String.valueOf(vals[1]);
            } else {
                value = applyNumeric(addition, BigDecimal::add);
            }
        }
        
        @Override
        public void visit(Division division) {
            value = applyNumeric(division, BigDecimal::divide);
        }

        @Override
        public void visit(Multiplication multiplication) {
            value = applyNumeric(multiplication, BigDecimal::multiply);
        }

        @Override
        public void visit(Subtraction subtraction) {
            value = applyNumeric(subtraction, BigDecimal::subtract);
        }

        @Override
        public void visit(AndExpression andExpression) {
            value = applyBoolean(andExpression, (a,b) -> a && b);
        }

        @Override
        public void visit(OrExpression orExpression) {
            value = applyBoolean(orExpression, (a,b) -> a || b);
        }

        @Override
        public void visit(Between between) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            Object[] vals = extractValues(equalsTo);
            value = Objects.equal(vals[0], vals[1]);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(InExpression inExpression) {
            // TODO Auto-generated method stub
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            // TODO Auto-generated method stub
        }

        @Override
        public void visit(MinorThan minorThan) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(Concat concat) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            // TODO Auto-generated method stub
            
        }
        
        @Override
        public void visit(CastExpression cast) {
            // TODO Auto-generated method stub
            
        }
        @Override
        public void visit(Modulo modulo) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(WithinGroupExpression wgexpr) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(ExtractExpression eexpr) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(IntervalExpression iexpr) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visit(RegExpMatchOperator rexpr) {
            // TODO Auto-generated method stub
            
        }
        
        public Object getValue() {
            return value;
        }
        
        private Boolean applyBoolean(BinaryExpression expression, BiFunction<Boolean, Boolean, Boolean> op) {
            Object[] vals = extractValues(expression);
            return op.apply(toBoolean(vals[0]), toBoolean(vals[1]));
        }

        private Number applyNumeric(BinaryExpression expression, BiFunction<BigDecimal, BigDecimal, BigDecimal> op) {
            Object[] vals = extractValues(expression);
            if(MathUtil.isNumber(vals[0]) && MathUtil.isNumber(vals[1])) {
                return MathUtil.apply((Number)vals[0], (Number)vals[1], op);
            }
            throw new UnsupportedOperationException("Cant perform " + expression + " on non-number values");
        }
        
        private Boolean toBoolean(Object value) {
            if(value instanceof Boolean) {
                return (Boolean) value;
            }
            return "true".equals(value) || "TRUE".equals(value);
        }

        private Object[] extractValues(BinaryExpression expression) {
            ValueExtractor left = new ValueExtractor();
            expression.getLeftExpression().accept(left);
            ValueExtractor right = new ValueExtractor();
            expression.getRightExpression().accept(right);
            return new Object[] {left.getValue(), right.getValue()};
        }

        private void unknown(Expression expression) {
            throw new UnsupportedOperationException("not supported " + expression);
        }

        private <I,O> O convert(I from, java.util.function.Function<I,O> converter) {
            return Optional.ofNullable(from)
                .map(converter::apply)
                .orElse(null);
        }

    }
    
   
}
