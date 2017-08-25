/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.measure.basic;

import org.apache.kylin.measure.ParamAsMeasureCount;

import java.math.BigDecimal;

public class BigDecimalSumIfAggFunc implements ParamAsMeasureCount {

    @Override
    public int getParamAsMeasureCount() {
        return 1;
    }

    public static BigDecimalSumAggregator init() {
        return new BigDecimalSumAggregator();
    }

    public static BigDecimalSumAggregator add(BigDecimalSumAggregator agg, Object value, Object key, Object keyValue) {
        if (key == null) {
            if (keyValue != null) {
                return agg;
            }
        } else if (!key.equals(keyValue)){
            return agg;
        }

        agg.aggregate((BigDecimal) value);
        return agg;
    }

    public static BigDecimalSumAggregator merge(BigDecimalSumAggregator agg, Object value, Object key, Object keyValue) {
        BigDecimalSumAggregator agg2 = (BigDecimalSumAggregator) value;
        if (agg2.getState() == null) {
            return agg;
        }
        return add(agg, agg2.getState(), key, keyValue);
    }

    public static BigDecimal result(BigDecimalSumAggregator agg) {
        BigDecimal finalState = agg.getState();
        return finalState == null ? BigDecimal.ZERO : finalState;
    }
}
