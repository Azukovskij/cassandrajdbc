/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.schema.Column;

public class ValueParser extends ExpressionVisitorAdapter {
    
    private List<Object> values = new ArrayList<Object>();;

    ValueParser() {}
    
    public static Function<Expression, Stream<Object>> instance() {
        return expr -> {
            ValueParser visitor = new ValueParser();
            expr.accept(visitor);
            return visitor.getValues();
        };
    }
    
    @Override
    public void visit(SignedExpression expr) {
        ValueParser.instance().apply(expr.getExpression())
            .map(number -> expr.getSign() == '+' ? number : negate(number, expr))
            .forEach(values::add);
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

    @Override
    public void visit(NullValue nullValue) {
        values.add(null);
    }

    @Override
    public void visit(StringValue stringValue) {
        values.add(stringValue.getValue());
    }
    
    @Override
    public void visit(DoubleValue doubleValue) {
        values.add(doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        values.add(longValue.getValue());
    }

    @Override
    public void visit(DateValue dateValue) {
        values.add(convert(dateValue.getValue(), v -> new Date(v.getTime())));
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        values.add(convert(timestampValue.getValue(), v -> new Date(v.getTime())));
    }
    
    @Override
    public void visit(TimeValue timeValue) {
        values.add(convert(timeValue.getValue(), v -> v.getTime()));
    }

    @Override
    public void visit(HexValue hexValue) {
        values.add(hexValue.getValue());
    }
    
    @Override
    public void visit(Column column) {
        switch (column.getColumnName()) {
            case "TRUE":
                values.add(true);
                break;
            case "FALSE":
                values.add(false);
                break;
            default:
                unknown(column);
                break;
        }
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
    
    public Stream<Object> getValues() {
        return values.stream();
    }

    private void unknown(Expression expression) {
    }

    private <I,O> O convert(I from, java.util.function.Function<I,O> converter) {
        return Optional.ofNullable(from)
            .map(converter::apply)
            .orElse(null);
    }
    


}
