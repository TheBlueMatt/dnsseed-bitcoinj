package com.mattcorallo.bitcoinjdnsseed;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.bitcoin.core.Sha256Hash;

/**
 * Copyright 2012 Matt Corallo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public abstract class DataStore {
    public static enum PeerState {
        // UNTESTED MUST be first
        UNTESTED,
        LOW_BLOCK_COUNT,
        HIGH_BLOCK_COUNT,
        LOW_VERSION,
        PEER_DISCONNECTED,
        NOT_FULL_NODE,
        TIMEOUT,
        TIMEOUT_DURING_REQUEST,
        GOOD,
        UNTESTABLE_ADDRESS
    }
    // The maximum length of a name in PeerState
    public static final int PEER_STATE_MAX_LENGTH = 22;
    
    // Retry times in milliseconds
    public Object retryTimesLock = new Object();
    public int[] retryTimes = new int[PeerState.values().length];
    
    // How far back in the chain to request the test block
    static final int MIN_BLOCK_OFFSET = 50;
    
    // Timeout is measured from initial connect attempt until a single block has been fully received (in seconds)
    public Object totalRunTimeoutLock = new Object();
    public int totalRunTimeout = 10;
    
    // New connection opened per second
    public Object connectionsPerSecondLock = new Object();
    public int connectionsPerSecond = 5;

    public DataStore() {
        synchronized(retryTimesLock) {
            retryTimes[PeerState.UNTESTED.ordinal()] =               -1 *60*60*1000; // Always try UNTESTED Nodes
            retryTimes[PeerState.LOW_BLOCK_COUNT.ordinal()] =         2 *60*60*1000; // Every 2 hours
            retryTimes[PeerState.HIGH_BLOCK_COUNT.ordinal()] =        2 *60*60*1000; // Every 2 hours
            retryTimes[PeerState.LOW_VERSION.ordinal()] =            48 *60*60*1000; // Every 2 days
            retryTimes[PeerState.PEER_DISCONNECTED.ordinal()] =      12 *60*60*1000; // Every 12 hours
            retryTimes[PeerState.NOT_FULL_NODE.ordinal()] =          96 *60*60*1000; // Every 4 days
            retryTimes[PeerState.TIMEOUT.ordinal()] =                48 *60*60*1000; // Every 2 days
            retryTimes[PeerState.TIMEOUT_DURING_REQUEST.ordinal()] = 12 *60*60*1000; // Every 12 hours
            retryTimes[PeerState.GOOD.ordinal()] =                    1 *60*60*1000; // Every hour
            retryTimes[PeerState.UNTESTABLE_ADDRESS.ordinal()] =      Integer.MAX_VALUE; // Never retest
        }
    }
    
    public abstract void addUpdateNode(InetSocketAddress addr, PeerState state);
    
    public abstract List<InetSocketAddress> getNodesToTest();
    
    public abstract int getMinBestHeight();
    
    public abstract void putHashAtHeight(int height, Sha256Hash hash);

    public abstract Sha256Hash getHashAtHeight(int height);

    public abstract String getStatus();
    
    public abstract int getNumberOfHashesStored();
}