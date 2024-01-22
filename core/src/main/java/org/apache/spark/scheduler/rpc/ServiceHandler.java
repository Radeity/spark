/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.rpc;

import org.apache.spark.scheduler.EarlyScheduleTracker;
import org.apache.thrift.TException;

public class ServiceHandler implements gurobiService.Iface{

    protected EarlyScheduleTracker earlyScheduleTracker;

    public ServiceHandler(EarlyScheduleTracker earlyScheduleTracker) {
        this.earlyScheduleTracker = earlyScheduleTracker;
    }

    @Override
    public Response1 gurobi(Request1 request) throws TException {
        return null;
    }

    @Override
    public void reportPartitionSize(Request2 request) throws TException {

    }

    @Override
    public void registerApplication(Request3 request) throws TException {

    }

    @Override
    public void deregisterApplication(Request4 request) throws TException {

    }

    @Override
    public void modelReady(Request5 request) {
        this.earlyScheduleTracker.makeScheduleDecision(request.mapStageId, request.numMappers,
                                                        request.numReducers, request.shuffleId);
    }

    @Override
    public void reportPartitionInputRow(Request6 request) throws TException {

    }

    @Override
    public ResponseM2 m2(Request7 request) throws TException {
        return null;
    }
}
