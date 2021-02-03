/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import junit.framework.AssertionFailedError;

public class CassandraURLTest {
    
    @Test
    public void shouldSkipUnsupportedScheme() {
        assertTrue(CassandraURL.create("jdbc:postgres://localhost/test").isEmpty());;
    }
    
    @Test
    public void shouldParseDefaultKeyspace() {
        CassandraURL testObject = parse("jdbc:cassandra://localhost/test");
        assertThat(testObject.getDefaultKeyspace(), equalTo("test"));
    }
    
    @Test
    public void shouldParseHosts() throws UnknownHostException {
        CassandraURL testObject = parse("jdbc:cassandra://localhost,127.0.0.1/test");
        assertThat(testObject.getContactPoints(), hasItems(
            InetAddress.getByName("localhost"),
            InetAddress.getByName("127.0.0.1")));
        assertThat(testObject.getContactPointsWithPorts(), empty());
    }

    @Test
    public void shouldParseHostsAndPorts() throws UnknownHostException {
        CassandraURL testObject = parse("jdbc:cassandra://localhost:9042,127.0.0.1:9043/test");
        assertThat(testObject.getContactPoints(), empty());
        assertThat(testObject.getContactPointsWithPorts(), hasItems(
            InetSocketAddress.createUnresolved("localhost", 9042),
            InetSocketAddress.createUnresolved("127.0.0.1", 9043)));
    }

    @Test
    public void shouldParseProperties() {
        CassandraURL testObject = parse("jdbc:cassandra://localhost/test?a=b&c=d");
        assertThat(testObject.getProperties(), hasEntry("a", "b"));
        assertThat(testObject.getProperties(), hasEntry("c", "d"));
    }

    private CassandraURL parse(String connectString) throws AssertionFailedError {
        return CassandraURL.create(connectString)
            .orElseThrow(() -> new AssertionFailedError("URI not parsed " + connectString));
    }
}
