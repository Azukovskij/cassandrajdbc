/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class StringMapCodec extends CodecDelegate<String, Map> {
    
    public StringMapCodec(CodecRegistry registry) {
        super(registry.codecFor(DataType.map(DataType.text(), DataType.text()), Map.of()), String.class);
    }

    @Override
    protected Map write(String value) {
        try {
            return (JSONObject) new JSONParser().parse(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected String read(Map value) {
        return JSONObject.toJSONString(value);
    }
    
}
