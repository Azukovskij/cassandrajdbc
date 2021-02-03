/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import java.util.Properties;

public class StatementOptions {
    
    public enum Collation { CASE_SENSITIVE, CASE_INSENSITIVE } 
    
    private int fetchSize;
    private int timeoutSec;
    private int limit;
    
    public Collation getCollation() {
        return Collation.CASE_INSENSITIVE;
    }
    
    public Properties getTableProps() {
        return null;
    }

}
