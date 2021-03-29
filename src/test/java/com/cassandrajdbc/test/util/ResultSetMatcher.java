/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.test.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class ResultSetMatcher<T> extends BaseMatcher<ResultSet> {

    private final Collection<Object> expected;
    private final CheckedFunction<ResultSet, T> mapper;
    private final int expectedCount;
    private List<?> lastResult;
    
    private ResultSetMatcher(Collection<Object> expected, CheckedFunction<ResultSet, T> mapper, int expectedCount) {
        this.expected = expected;
        this.mapper = mapper;
        this.expectedCount = expectedCount;
    }
    
    public static Matcher<ResultSet> hasResultCount(int expectedCount) {
        return new ResultSetMatcher<>(null, rs -> rs.getObject(1), expectedCount);
    }
    
    public static <T> Matcher<ResultSet> hasResultItems(Collection<Object> expected, CheckedFunction<ResultSet, T> extractor) {
        HashSet<Object> distinct = new HashSet<>(expected);
        return new ResultSetMatcher<>(distinct, extractor, distinct.size());
    }
    
    public static <T> Matcher<ResultSet> resultsEqualTo(List<Object> expected, CheckedFunction<ResultSet, T> extractor) {
        return new ResultSetMatcher<>(expected, extractor, expected.size());
    }

    @Override
    public boolean matches(Object object) {
        if(object == null) {
            return false;
        }
        try {
            List<T> actual = readFully((ResultSet) object);
            lastResult = actual;
            if(expected == null) {
                return expectedCount == actual.size();
            }
            return expected instanceof List ? expected.equals(actual) :
                actual.containsAll(expected);
        } catch (SQLException e) {
            throw new AssertionError("failed to get results", e);
        }
    }

    @Override
    public void describeTo(Description description) {
        if(expected == null) {
            description.appendText("ResultSet with ").appendValue(expectedCount).appendText(" elements, got " + lastResult.size());
        } else {
            description.appendText("ResultSet containing ").appendValue(expected).appendText(", got " + lastResult);
        }
    }

    private List<T> readFully(ResultSet rs) throws SQLException {
        List<T> values = new ArrayList<>();
        while (rs.next()) {
            values.add(mapper.apply(rs));
        }
        return values;
    }

    @FunctionalInterface
    public interface CheckedFunction<T,R> {
        
        R apply(T request) throws SQLException;
        
    }

}
