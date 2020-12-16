/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.functionNamespace.execution.thrift;

import com.facebook.airlift.log.Logger;
import com.facebook.drift.TException;
import com.facebook.drift.client.DriftClient;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.ThriftScalarFunctionImplementation;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.facebook.presto.thrift.api.udf.ThriftFunctionHandle;
import com.facebook.presto.thrift.api.udf.ThriftUdfService;
import com.facebook.presto.thrift.api.udf.ThriftUdfServiceException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.thriftPage;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

public class ThriftSqlFunctionExecutor
{
    private final DriftClient<ThriftUdfService> thriftUdfClient;

    @Inject
    public ThriftSqlFunctionExecutor(DriftClient<ThriftUdfService> thriftUdfClient)
    {
        this.thriftUdfClient = thriftUdfClient;
    }

    public CompletableFuture<Block> executeFunction(ThriftScalarFunctionImplementation functionImplementation, Page input, List<Integer> channels, List<Type> argumentTypes, Type returnType)
    {
        if (thriftUdfClient == null) {
            throw new UnsupportedOperationException("Thrift function execution is not supported");
        }
        maybeAddGcPressure();
        ImmutableList.Builder<PrestoThriftBlock> blocks = ImmutableList.builder();
        for (int i = 0; i < channels.size(); i++) {
            Block block = input.getBlock(channels.get(i));
            blocks.add(PrestoThriftBlock.fromBlock(block, argumentTypes.get(i)));
        }
        SqlFunctionHandle functionHandle = functionImplementation.getFunctionHandle();
        SqlFunctionId functionId = functionHandle.getFunctionId();
        try {
            return toCompletableFuture(thriftUdfClient.get(Optional.of(functionImplementation.getLanguage().getLanguage())).invokeUdf(
                    new ThriftFunctionHandle(
                            functionId.getFunctionName().toString(),
                            functionId.getArgumentTypes().stream()
                                    .map(TypeSignature::toString)
                                    .collect(toImmutableList()),
                            returnType.toString(),
                            functionHandle.getVersion()),
                    thriftPage(blocks.build())))
                    .thenApply(result -> getOnlyElement(result.getResult().getThriftBlocks()).toBlock(returnType));
        }
        catch (ThriftUdfServiceException | TException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static final Logger log = Logger.get(ThriftSqlFunctionExecutor.class);

    private static final boolean shouldAddGcPressure;
    static {
        log.warn("Configuring gcPressure");
        String taskId = System.getenv().getOrDefault("TW_TASK_ID", "0");
        shouldAddGcPressure = "1".equalsIgnoreCase(taskId);
        log.warn("Got taskId: " + taskId + " shouldAddGcPressure: " + shouldAddGcPressure);
    }

    private static final ThreadLocal<LinkedList<byte[]>> gcFuzzer = new ThreadLocal<>();
    private static final Random rnd = new Random();

    private static final int garbageCycleLength = new Integer(System.getProperty("garbageCycleLength", "1000"));
    static {
        if (shouldAddGcPressure) {
            log.warn("gcPressure will work with garbageCycleLength: " + garbageCycleLength);
        }
    }

    private static void maybeAddGcPressure()
    {
        // 1 in 1000 we make the garbage collectible
        if (rnd.nextInt(garbageCycleLength) == 0) {
            List<byte[]> lst = gcFuzzer.get();
            gcFuzzer.remove();
            if (lst != null) {
                long memReleased = lst.stream().collect(Collectors.summingLong(x -> x.length));
                log.warn("GC PRESSURIZER Releasing memory: " + (memReleased / 1024 / 1024) + " mb. ");
            }
        }
        else {
            // add random garbage in 1-10mb range
            LinkedList<byte[]> list = gcFuzzer.get();
            if (list == null) {
                list = new LinkedList<>();
                gcFuzzer.set(list);
            }
            list.add(new byte[(new Random().nextInt(10) + 1) * 1024 * 1024]);
        }
    }
}
