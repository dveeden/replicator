package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 * @author Jingqi Xu
 */

/**
 * Copied from: https://raw.githubusercontent.com/whitesock/open-replicator/master/src/main/java/com/google/code/or/common/glossary/column/TinyColumn.java
 *              and renamed TinyColumn to TinyCell
 */
public final class TinyCell implements Cell {
    //
    private static final long serialVersionUID = 3629858638897033423L;

    //
    public static final int MIN_VALUE = -128;
    public static final int MAX_VALUE = 127;

    //
    private static final TinyCell[] CACHE = new TinyCell[256];
    static {
        for(int i = MIN_VALUE; i <= MAX_VALUE; i++) {
            CACHE[i + 128] = new TinyCell(i);
        }
    }

    //
    private final int value;

    /**
     *
     */
    private TinyCell(int value) {
        this.value = value;
    }

    /**
     *
     */
    @Override
    public String toString() {
        return String.valueOf(this.value);
    }

    /**
     *
     */
    public Integer getValue() {
        return this.value;
    }

    /**
     *
     */
    public static final TinyCell valueOf(int value) {
        if(value < MIN_VALUE || value > MAX_VALUE) throw new IllegalArgumentException("invalid value: " + value);
        return CACHE[value + 128];
    }
}