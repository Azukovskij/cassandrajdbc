/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.cassandrajdbc.util.Either;

public final class CassandraURL {
    
    private static final String QUERY_STR_SEPARATOR = "\\?";
    private static final String KEYSPACE_SEPARATOR = "/";
    private static final String CONTACT_POINT_SEPARATOR = ",";
    private static final String SCHEME = "jdbc:cassandra://";
    
    private final List<Either<InetAddress, InetSocketAddress>> contactPoints;
    private final String defaultKeyspace;
    private final Properties properties;
    private final String rawUrl;
    
    public static Optional<CassandraURL> create(String connectionUri) {
        return Optional.ofNullable(connectionUri)
            .filter(uri -> uri.startsWith(SCHEME))
            .map(uri -> uri.substring(SCHEME.length()).split(QUERY_STR_SEPARATOR))
            .flatMap(p1 -> Optional.of(p1[0])
                .map(cp -> cp.split(KEYSPACE_SEPARATOR))
                .map(p2 -> new CassandraURL(connectionUri, p2[0], secondValue(p2), secondValue(p1))));
    }

    private static String secondValue(String[] array) {
        return array.length < 2 ? null : array[1];
    }

    private CassandraURL(String rawUrl, String contactPoints, String defaultKeyspace, String queryString) {
        this.rawUrl = rawUrl;
        this.contactPoints = Arrays.stream(contactPoints.split(CONTACT_POINT_SEPARATOR))
            .map(this::parseHost)
            .collect(Collectors.toList());
        this.defaultKeyspace = defaultKeyspace;
        this.properties = Optional.ofNullable(queryString).stream()
            .flatMap(qs -> Arrays.stream(qs.split("&")))
            .map(entry -> entry.split("="))
            .collect(Collectors.toMap(e -> e[0], e -> e[1], (a,b) -> b, Properties::new));
    }

    private Either<InetAddress, InetSocketAddress> parseHost(String address) {
        try {
            String[] parts = address.split(":");
            return parts.length == 2 
                ? Either.ofOther(InetSocketAddress.createUnresolved(parts[0], Integer.parseInt(parts[1])))
                : Either.ofOne(InetAddress.getByName(parts[0]));
        } catch (UnknownHostException e1) {
            throw new IllegalArgumentException("Invalid contact point " + address, e1);
        }
    }
    
    public String getDefaultKeyspace() {
        return defaultKeyspace;
    }
    
    public List<InetAddress> getContactPoints() {
        return contactPoints.stream()
            .flatMap(cp -> cp.getOne().stream())
            .collect(Collectors.toUnmodifiableList());
    }
    
    public List<InetSocketAddress> getContactPointsWithPorts() {
        return contactPoints.stream()
            .flatMap(cp -> cp.getOther().stream())
            .collect(Collectors.toUnmodifiableList());
    }
    
    public Properties getProperties() {
        return properties;
    }
    
    public String getRawUrl() {
        return rawUrl;
    }

}
