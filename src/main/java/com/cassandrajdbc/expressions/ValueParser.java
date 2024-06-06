/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.cassandrajdbc.util.MathUtil;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Objects;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
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
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.schema.Column;

public class ValueParser extends ExpressionVisitorAdapter {
    
    private Object value = null;  

    protected ValueParser() {}
    
    public static Function<Expression, Object> instance() {
        return expr -> {
            ValueParser visitor = new ValueParser();
            expr.accept(visitor);
            
            return visitor.getValue();
        };
    }
    
    public static Function<Expression, Object> instance(ColumnMetadata col) {
        return expr -> {
            ValueParser visitor = new ValueParser();
            expr.accept(visitor);
            
            return convertInternal(col, visitor.getValue());
        };
    }
    

    
    private static Object convertInternal(ColumnMetadata col, Object val) {
        if(col.getType() == DataType.uuid() && val instanceof String) {
            return UUID.fromString((String) val);
        }
        if(col.getType() == DataType.date() && val instanceof String) {
            java.time.LocalDate date = java.time.LocalDate.parse((CharSequence) val);
            return LocalDate.fromYearMonthDay(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        }
        if(col.getType() == DataType.timestamp() && val instanceof String) {
            return Date.from(parseTime((String) val));
        }
        if(DataType.Name.MAP.equals(col.getType().getName()) && val instanceof String) {
            try {
                return new JSONParser().parse((String) val);
            } catch (ParseException e) {
                return val;
            }
        }
        if(col.getType() == DataType.blob() && val instanceof String) {
            return ByteBuffer.wrap(val.toString().getBytes(StandardCharsets.UTF_8));
        }
        return val;
    }

    private static Instant parseTime(String string) {
        try {
            return ZonedDateTime.parse((String)string).toInstant();
        } catch (DateTimeParseException e) {
            return LocalDateTime.parse((String) string).toInstant(ZoneOffset.UTC);
        }
    }
    
    @Override
    public void visit(SignedExpression expr) {
        Object number = ValueParser.instance().apply(expr.getExpression());
        value = expr.getSign() == '+' ? number : negate(number, expr);
    }

    @Override
    public void visit(NullValue nullValue) {
        value = null;
    }

    @Override
    public void visit(StringValue stringValue) {
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
        value = QueryBuilder.bindMarker();
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
    public void visit(net.sf.jsqlparser.expression.Function function) {
        unknown(function);
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

    private Object negate(Object number, SignedExpression expr) {
        if(number instanceof Integer) {
            return Math.negateExact((int) number);
        }
        if(number instanceof Long) {
            return Math.negateExact((long) number);
        }
        throw new UnsupportedOperationException("Unable to negate " + expr);
    }

    private Object[] extractValues(BinaryExpression expression) {
        ValueParser left = new ValueParser();
        expression.getLeftExpression().accept(left);
        ValueParser right = new ValueParser();
        expression.getRightExpression().accept(right);
        return new Object[] {left.getValue(), right.getValue()};
    }

    private <I,O> O convert(I from, java.util.function.Function<I,O> converter) {
        return Optional.ofNullable(from)
            .map(converter::apply)
            .orElse(null);
    }

    private void unknown(Expression expression) {
        throw new UnsupportedOperationException("not supported " + expression);
    }

}
