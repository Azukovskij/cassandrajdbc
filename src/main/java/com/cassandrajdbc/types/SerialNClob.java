/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types;

import java.sql.NClob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

public class SerialNClob extends SerialClob implements NClob {

    private static final long serialVersionUID = -4192763025622238750L;

    public SerialNClob(char[] ch) throws SerialException, SQLException {
        super(ch);
    }

}
