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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.api.TokenSupplier;

/**
 * Static factory holder that provides a token supplier
 */
public class TestTokenSupplier
{

    /**
     * Tokens are allocation used in tests to simulate token allocation nodes for an approx even distribution
     * in a multiDC environment with neighboring nodes different DCs allocated adjacent tokens.
     * Allocations for new nodes are interleaved among existing tokens.
     * @param numNodesPerDC no. nodes from a single DC
     * @param numDcs no. of datacenters
     * @param numTokensPerNode no. tokens allocated to each node (this is always 1 if there are no vnodes)
     * @return The token supplier that vends the tokens
     */
    static TokenSupplier evenlyDistributedTokens(int numNodesPerDC, int newNodesPerDC, int numDcs, int numTokensPerNode)
    {
        // Use token count using initial node count to first assign tokens to nodes
        long totalTokens = (long) (numNodesPerDC) * numDcs * numTokensPerNode;
        // Similar to Cassandra TokenSupplier, the increment is doubled to account for all tokens from MIN - MAX.
        // For multi-DC, since neighboring nodes from different DCs have consecutive tokens, the increment is
        // broadened by a factor of numDcs.
        BigInteger increment = BigInteger.valueOf(((Long.MAX_VALUE / (totalTokens + 2)) * 2 * numDcs));
        List<String>[] tokens = allocateExistingNodeTokens(numNodesPerDC,
                                                           newNodesPerDC,
                                                           numDcs,
                                                           numTokensPerNode,
                                                           increment);


        // Initial value of the first new node
        BigInteger value  = new BigInteger(tokens[0 + (numDcs - 1)].get(0));
        BigInteger subIncrement = increment.divide(BigInteger.valueOf(2));

        int nodeId = (int) totalTokens + 1;
        for (int i = 0; i < numTokensPerNode; ++i)
        {
            while (nodeId <= ((numNodesPerDC + newNodesPerDC) * numDcs))
            {
                value = value.add(subIncrement);
                // Nodes in different DCs are separated by a single token
                for (int dc = 0; dc < numDcs; dc++)
                {
                    tokens[nodeId - 1].add(value.add(BigInteger.valueOf(dc)).toString());
                    nodeId++;
                }
                value = value.add(subIncrement);
            }
        }

        return (nodeIdx) -> tokens[nodeIdx - 1];
    }

    private static List<String>[] allocateExistingNodeTokens(int numNodesPerDC,
                                                             int newNodesPerDC,
                                                             int numDcs,
                                                             int numTokensPerNode,
                                                             BigInteger increment)
    {
        List<String>[] tokens = new List[(numNodesPerDC + newNodesPerDC) * numDcs];
        for (int i = 0; i < ((numNodesPerDC + newNodesPerDC) * numDcs); ++i)
        {
            tokens[i] = new ArrayList(numTokensPerNode);
        }

        BigInteger value = BigInteger.valueOf(Long.MIN_VALUE + 1);

        int nodeId = 1;
        for (int i = 0; i < numTokensPerNode; ++i)
        {
            while (nodeId <= (numNodesPerDC * numDcs))
            {
                value = value.add(increment);
                // Nodes in different DCs are separated by a single token
                for (int dc = 0; dc < numDcs; dc++)
                {
                    tokens[nodeId - 1].add(value.add(BigInteger.valueOf(dc)).toString());
                    nodeId++;
                }
            }
        }
        return tokens;
    }
}
