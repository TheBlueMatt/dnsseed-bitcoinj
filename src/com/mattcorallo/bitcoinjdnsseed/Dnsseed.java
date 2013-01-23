package com.mattcorallo.bitcoinjdnsseed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.SimpleFormatter;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import com.google.bitcoin.core.AbstractPeerEventListener;
import com.google.bitcoin.core.AddressMessage;
import com.google.bitcoin.core.AlertMessage;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.BlockChain;
import com.google.bitcoin.core.GetBlocksMessage;
import com.google.bitcoin.core.GetAddrMessage;
import com.google.bitcoin.core.GetDataMessage;
import com.google.bitcoin.core.InventoryItem;
import com.google.bitcoin.core.InventoryMessage;
import com.google.bitcoin.core.Message;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.Peer;
import com.google.bitcoin.core.PeerAddress;
import com.google.bitcoin.core.PeerGroup;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.VersionMessage;
import com.google.bitcoin.discovery.DnsDiscovery;
import com.google.bitcoin.discovery.PeerDiscoveryException;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.store.BlockStoreException;
import com.google.bitcoin.store.BoundedOverheadBlockStore;

public class Dnsseed {
    static class ChannelFutureAndProgress {
        public ChannelFuture channel;
        public Sha256Hash targetHash;
        public boolean hasReceivedAddressMessage = false;
        public boolean hasPassedBlockDownloadTest = false;
        public boolean hasAskedForBlocks = false;
        public boolean hasPassed = false;
        public DataStore.PeerState timeoutState = DataStore.PeerState.TIMEOUT;
        ChannelFutureAndProgress(ChannelFuture channel) { this.channel = channel; }
    }
    static HashMap<Peer, ChannelFutureAndProgress> peerToChannelMap = new HashMap<Peer, ChannelFutureAndProgress>();
    static PeerGroup peerGroup;
    static final NetworkParameters params = NetworkParameters.prodNet();
    static DataStore store;
    static File blockChainFile;
    static BlockStore blockStore;
    static BlockChain chain;
    
    static Object exitableLock = new Object();
    static int exitableSemaphore = 0;
    
    static Object scanLock = new Object();
    static boolean scanable = false;
    
    static Object updateStatsLock = new Object();
    static boolean printNodeCounts = true;
    static boolean refreshStats = true;
    
    static Object statusLock = new Object();
    static int numRoundsComplete = 0;
    static int numScansThisRound = 0;
    static int numScansCompletedThisRound = 0;
    static boolean isWaitingForEmptyPeerToStatusMap = false;
    
    static final int MAX_BLOCKS_AHEAD = 25;
    
    static final int DUMP_DATASTORE_PERIOD_SECONDS = 60 * 30; // Every 30 minutes
    static final int DUMP_DATASTORE_NODES_PERIOD_MULTIPLIER = 60 * 60 * 6 / DUMP_DATASTORE_PERIOD_SECONDS; // Every 6 hours (DUMP_DATASTORE_PERIOD_SECONDS * DUMP_DATASTORE_NODES_PERIOD_MULTIPLIER)
    
    // Timeout before we have the peer's version message (in seconds)
    static final int CONNECT_TIMEOUT = 5;
    
    static LinkedList<String> logList = new LinkedList<String>();
    static FileOutputStream logFileStream;
    
    private static void PauseScanning() {
        synchronized(scanLock) {
            scanable = false;
        }
        synchronized(peerToChannelMap) {
            while (!peerToChannelMap.isEmpty())
                try {
                    peerToChannelMap.wait();
                } catch (InterruptedException e) {
                    ErrorExit(e);
                }
        }
    }
    
    private static void ContinueScanning() {
        synchronized(scanLock) {
            scanable = true;
            scanLock.notifyAll();
        }
    }
        
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        System.out.println("USAGE: Dnsseed datastore localPeerAddress");
        if (args.length != 2)
            System.exit(1);
        
        org.jboss.netty.logging.InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        
        Handler fileHandlerWarn = new FileHandler(args[0] + "/warn.log");
        fileHandlerWarn.setLevel(Level.WARNING);
        fileHandlerWarn.setFormatter(new SimpleFormatter());
        Handler fileHandlerAll = new FileHandler(args[0] + "/full.log");
        fileHandlerAll.setLevel(Level.ALL);
        fileHandlerAll.setFormatter(new SimpleFormatter());
        LogManager.getLogManager().reset();
        Enumeration<String> enumeration = LogManager.getLogManager().getLoggerNames();
        while (enumeration.hasMoreElements()) {
            String name = enumeration.nextElement();
            LogManager.getLogManager().getLogger(name).addHandler(fileHandlerWarn);
            LogManager.getLogManager().getLogger(name).addHandler(fileHandlerAll);
        }
        
        logFileStream = new FileOutputStream(args[0] + "/status.log");
        for (int i = 0; i < 25; i++)
            logList.add(i, null);
        
        blockStore = new BoundedOverheadBlockStore(params, new File(args[0] + "/dnsseed.chain"));
        store = new MemoryDataStore(args[0] + "/memdatastore", blockStore);
        
        InitPeerGroup(args[1]);
        LaunchAddNodesThread();
        LaunchStatsPrinterThread();
        LaunchDumpGoodAddressesThread(args[0] + "/nodes.dump");
        LaunchBackupDataStoreThread();
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String line = reader.readLine();
        while (line != null) {
            if (line.equals("q")) {
                // Used to make sure block height -> hash mappings dont get out of sync with block db itself
                synchronized (exitableLock) {
                    while (exitableSemaphore > 0)
                        exitableLock.wait();
                    PauseScanning();
                    peerGroup.stop();
                    if (store instanceof MemoryDataStore) {
                        ((MemoryDataStore)store).saveNodesState();
                        ((MemoryDataStore)store).saveConfigAndBlocksState();
                    }
                    logFileStream.close();
                    System.exit(0);
                }
            } else if (line.length() >= 5 && line.charAt(0) == 'r' && line.charAt(1) == ' ') {
                String[] values = line.split(" ");
                if (values.length == 3) {
                    try {
                        int index = Integer.parseInt(values[1]);
                        int value = Integer.parseInt(values[2]);
                        synchronized(store.retryTimesLock) {
                            try {
                                store.retryTimes[index] = value * 60;
                            } catch (IndexOutOfBoundsException e) {
                                LogLine("Invalid status code");
                            }
                        }
                    } catch (NumberFormatException e) {
                        LogLine("Invalid argument");
                    }
                }
            } else if (line.length() >= 3 && line.charAt(0) == 'c' && line.charAt(1) == ' ') {
                String[] values = line.split(" ");
                if (values.length == 2) {
                    try {
                        synchronized(store.connectionsPerSecondLock) {
                            store.connectionsPerSecond = Integer.parseInt(values[1]);
                        }
                    } catch (NumberFormatException e) {
                        LogLine("Invalid argument");
                    }
                }
            } else if(line.length() >= 3 && line.charAt(0) == 't' && line.charAt(1) == ' ') {
                String[] values = line.split(" ");
                if (values.length == 2) {
                    try {
                        synchronized(store.totalRunTimeoutLock) {
                            store.totalRunTimeout = Integer.parseInt(values[1]);
                        }
                    } catch (NumberFormatException e) {
                        LogLine("Invalid argument");
                    }
                }
            } else if(line.length() >= 3 && line.charAt(0) == 'w' && line.charAt(1) == ' ') {
                String[] values = line.split(" ");
                if (values.length == 2) {
                    try {
                        synchronized(store.retryTimesLock) {
                            store.ageOfLastSuccessToRetryAsGood = Integer.parseInt(values[1]) * 60;
                        }
                    } catch (NumberFormatException e) {
                        LogLine("Invalid argument");
                    }
                }
            } else if (line.equals("n")) {
                synchronized(updateStatsLock) {
                    printNodeCounts = !printNodeCounts;
                }
            } else if (line.equals("p")) {
                synchronized(updateStatsLock) {
                    refreshStats = !refreshStats;
                    updateStatsLock.notifyAll();
                }
            } else {
                LogLine("Invalid command/arguments");
            }
            line = reader.readLine();
        }
    }

    static void ErrorExit(String message) {
        ErrorExit(new Exception(message));
    }
    
    static void ErrorExit(Exception exception) {
        synchronized (exitableLock) {
            while (exitableSemaphore > 0)
                try {
                    exitableLock.wait();
                } catch (InterruptedException e) {
                    System.err.println("ErrorExit got InterruptedException!");
                    if (exception != null)
                        exception.printStackTrace();
                    else
                        new Throwable().printStackTrace();
                    System.exit(1);
                }
            System.err.println("ErrorExit() called:");
            if (exception != null)
                exception.printStackTrace();
            else
                new Throwable().printStackTrace();
            System.exit(1);
        }
    }
    
    private static void LaunchDumpGoodAddressesThread(final String fileName) { // In partial BIND Zonefile format
        final String preEntry = "@\tIN\t";
        final String preIPv4Entry = "A\t";
        final String preIPv6Entry = "AAAA\t";
        final String postEntry = "\n";
        new Thread() {
            public void run() {
                while (true) {
                    try {
                        FileOutputStream file = new FileOutputStream(fileName + ".tmp");
                        // We grab the top 25 most recently tested nodes
                        for (InetAddress address : store.getMostRecentGoodNodes(25, params.port)) {
                            String line = null;
                            if (address instanceof Inet4Address)
                                line = preEntry + preIPv4Entry + address.getHostAddress() + postEntry;
                            else if (address instanceof Inet6Address)
                                line = preEntry + preIPv6Entry + address.getHostAddress() + postEntry;
                            else
                                ErrorExit("Unknown address type");
                            file.write(line.getBytes());
                        }
                        file.close();
                        new File(fileName).delete();
                        new File(fileName + ".tmp").renameTo(new File(fileName));
                    } catch (IOException e) {
                        ErrorExit(e);
                    }
                    try {
                        Thread.sleep(120*1000);
                    } catch (InterruptedException e) { ErrorExit(e); }
                }
            }
        }.start();
    }
    
    private static void LaunchBackupDataStoreThread() {
        new Thread(new Runnable() {
            public void run() {
                if (store instanceof MemoryDataStore) {
                    for (int i = 0; true; i++) {
                        try {
                            Thread.sleep(1000 * DUMP_DATASTORE_PERIOD_SECONDS);
                        } catch (InterruptedException e) {
                            ErrorExit(e);
                        }
                        synchronized (exitableLock) {
                            exitableSemaphore++;
                        }
                        PauseScanning();
                        if (i % DUMP_DATASTORE_NODES_PERIOD_MULTIPLIER == DUMP_DATASTORE_NODES_PERIOD_MULTIPLIER - 1) {
                            LogLine("Saving DataStore nodes state.");
                            ((MemoryDataStore) store).saveNodesState();
                        }
                        LogLine("Saving DataStore blocks state.");
                        ((MemoryDataStore) store).saveConfigAndBlocksState();
                        ContinueScanning();
                        synchronized (exitableLock) {
                            exitableSemaphore--;
                            exitableLock.notifyAll();
                        }
                    }
                }
            }
        }).start();
    }
    
    private static void LaunchStatsPrinterThread() {
        new Thread() {
            public void run() {
                while (true) {
                    synchronized (exitableLock) {
                        exitableSemaphore++;
                    }
                    //Pre-loaded values
                    int hashesStored = store.getNumberOfHashesStored();
                    int totalRunTimeoutCache;
                    synchronized(store.totalRunTimeoutLock) {
                        totalRunTimeoutCache = store.totalRunTimeout;
                    }
                    System.out.print("\033[2J\033[;H");
                    System.out.println();
                    synchronized(logList) {
                        for (String line : logList) {
                            if (line != null)
                                System.out.println(line);
                        }
                    }
                    System.out.println();
                    synchronized(updateStatsLock) {
                        if (printNodeCounts) {
                            System.out.println("Node counts by status:");
                            System.out.println(store.getStatus());
                            System.out.println();
                        }
                    }
                    synchronized (peerToChannelMap) {
                        System.out.println("Current connections open/in progress: " + peerToChannelMap.size());
                    }
                    synchronized (store.connectionsPerSecondLock) {
                        System.out.println("Connections opened each second: " + store.connectionsPerSecond);
                    }
                    synchronized (statusLock) {
                        System.out.println("This round of scans: " + numScansCompletedThisRound + "/" + numScansThisRound +
                                (isWaitingForEmptyPeerToStatusMap ? " (waiting for final cleanup before next round)" : ""));
                        //System.out.println("Number of rounds of scans completed: " + numRoundsComplete);
                    }
                    System.out.println("Current block count: " + chain.getBestChainHeight() + " == " + hashesStored);
                    System.out.println("Timeout for full run (in seconds): " + totalRunTimeoutCache);
                    System.out.println();
                    System.out.println("Retry times (in minutes):");
                    synchronized (store.retryTimesLock) {
                        for (DataStore.PeerState state : DataStore.PeerState.values()) {
                            System.out.print(state.name() + " (" + state.ordinal() + "): ");
                            for (int i = DataStore.PEER_STATE_MAX_LENGTH - (state.ordinal() > 9 ? 1 : 0); i > state.name().length(); i--)
                                System.out.print(" ");
                            System.out.println(store.retryTimes[state.ordinal()] / 60);
                        }
                        System.out.println("Consider a node WAS_GOOD after failure for " + (store.ageOfLastSuccessToRetryAsGood / 60) + " minutes (+1 update).");
                    }
                    System.out.println();
                    System.out.println("Commands:");
                    System.out.println("q: quit");
                    System.out.println("r x y: Change retry time for status x (int value, see retry times section for name mappings) to y (in hours)");
                    System.out.println("w x: Change the amount of time a node is considered WAS_GOOD after it fails to x (in hours)");
                    System.out.println("c x: Change connections opened per second to x");
                    System.out.println("t x: Change full run timeout to x seconds");
                    System.out.println("n: Enable/disable printing node counts");
                    System.out.println("p: Enable/disable updating these stats");
                    System.out.print("\n\033[s"); // Save cursor position and provide a blank line before cursor
                    System.out.print("\033[;H\033[2K");
                    System.out.println("Most recent log:");
                    System.out.print("\033[u\033[1A"); // Restore cursor position and go up one line
                    synchronized (exitableLock) {
                        exitableSemaphore--;
                        exitableLock.notifyAll();
                    }
                    try {
                        Thread.sleep(1000);
                        synchronized(updateStatsLock) {
                            while (!refreshStats)
                                updateStatsLock.wait();
                        }
                    } catch (InterruptedException e) {
                        ErrorExit(e);
                    }
                }
            }
        }.start();
    }
    
    private static void LaunchAddNodesThread() {
        new Thread() {
            public void run() {
                while (true) {
                    List<InetSocketAddress> addressesToTest = store.getNodesToTest();
                    synchronized(statusLock) {
                        numScansThisRound = addressesToTest.size();
                        numScansCompletedThisRound = 0;
                    }
                    for (final InetSocketAddress addr : addressesToTest) {
                        if (addr.getAddress().isLoopbackAddress() || addr.getAddress().isSiteLocalAddress() ||
                                addr.getAddress().isMulticastAddress() ||
                                addr.getAddress() instanceof Inet6Address) // TODO: Get IPv6
                            store.addUpdateNode(addr, DataStore.PeerState.UNTESTABLE_ADDRESS);
                        else
                            ScanHost(addr);
                        synchronized(statusLock) {
                            numScansCompletedThisRound++;
                        }
                        try {
                            int sleepTime;
                            synchronized(store.connectionsPerSecondLock) {
                                sleepTime = 1000/store.connectionsPerSecond;
                            }
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            ErrorExit(e);
                        }
                    }
                    synchronized(statusLock) {
                        if (numScansThisRound > 0)
                            numRoundsComplete++;
                    }
                    synchronized(statusLock) {
                        isWaitingForEmptyPeerToStatusMap = true;
                    }
                    synchronized(peerToChannelMap) {
                        while (!peerToChannelMap.isEmpty())
                            try {
                                peerToChannelMap.wait();
                            } catch (InterruptedException e) {
                                ErrorExit(e);
                            }
                    }
                    synchronized(statusLock) {
                        isWaitingForEmptyPeerToStatusMap = false;
                    }
                    try {
                        Thread.sleep(1 * 1000);
                    } catch (InterruptedException e) {
                        ErrorExit(e);
                    }
                }
            }
        }.start();
    }
    
    private static void InitPeerGroup(final String localPeerAddress) throws BlockStoreException, UnknownHostException {
        chain = new BlockChain(params, blockStore);
        peerGroup = new PeerGroup(params, chain);
        peerGroup.setUserAgent("DNSSeed", ">9000");
        peerGroup.setFastCatchupTimeSecs(Long.MAX_VALUE);
        peerGroup.start();
        
        ChannelFuture channelFuture = peerGroup.connectTo(new InetSocketAddress(InetAddress.getByName(localPeerAddress), params.port));
        final Peer localPeerOutside = PeerGroup.peerFromChannelFuture(channelFuture);
        
        peerGroup.addEventListener(new AbstractPeerEventListener() {
            Peer localPeer = localPeerOutside;
            @Override
            public void onPeerConnected(Peer peer, int peerCount) {
                if (peer == localPeer) {
                    if (peer.getBestHeight() == chain.getBestChainHeight())
                        StartScan(peer);
                    try {
                        peer.startBlockChainDownload();
                    } catch (IOException e) {
                        ErrorExit(e);
                    }
                    return;
                }
                synchronized(peerToChannelMap) {
                    if (peerToChannelMap.get(peer) == null)
                        throw new RuntimeException("Illegal state 3 (forcing peer disconnect in onPeerConnected)!");
                }
                DataStore.PeerState disconnectReason = null;
                if ((peer.getPeerVersionMessage().localServices & VersionMessage.NODE_NETWORK) != VersionMessage.NODE_NETWORK)
                    disconnectReason = DataStore.PeerState.NOT_FULL_NODE;
                if (peer.getVersionMessage().clientVersion < 40000)
                    disconnectReason = DataStore.PeerState.LOW_VERSION;
                if (peer.getBestHeight() < store.getMinBestHeight())
                    disconnectReason = DataStore.PeerState.LOW_BLOCK_COUNT;
                try {
                    if (peer.getBestHeight() > chain.getBestChainHeight() + MAX_BLOCKS_AHEAD)
                        disconnectReason = DataStore.PeerState.HIGH_BLOCK_COUNT;
                } catch (IllegalStateException e) {
                    disconnectReason = DataStore.PeerState.NOT_FULL_NODE;
                }
                if (disconnectReason != null) {
                    AsyncUpdatePeer(peer, disconnectReason);
                    return;
                }
                try {
                    peer.sendMessage(new GetAddrMessage(params));
                    int targetHeight = store.getMinBestHeight();
                    peer.sendMessage(new GetBlocksMessage(params, Arrays.asList(store.getHashAtHeight(targetHeight - 1)),
                            store.getHashAtHeight(targetHeight + 1)));
                    synchronized(peerToChannelMap) {
                        ChannelFutureAndProgress peerState = peerToChannelMap.get(peer);
                        peerState.targetHash = store.getHashAtHeight(targetHeight);;
                        peerState.timeoutState = DataStore.PeerState.TIMEOUT_DURING_REQUEST;
                    }
                } catch (IOException e) {
                    AsyncUpdatePeer(peer, DataStore.PeerState.PEER_DISCONNECTED);
                    throw new RuntimeException(e);
                }
            }
            
            @Override
            public void onPeerDisconnected(Peer peer, int peerCount) {
                if (peer == localPeer) {
                    new Thread(new Runnable() {
                        public void run() {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e1) { }
                            LogLine("Reconnecting to local peer after onPeerDisconnected");
                            ChannelFuture channelFuture;
                            try {
                                channelFuture = peerGroup.connectTo(new InetSocketAddress(InetAddress.getByName(localPeerAddress), params.port));
                            } catch (UnknownHostException e) {
                                ErrorExit("UnknownHostException trying to reconnect to localPeer");
                                throw new RuntimeException(e);
                            }
                            localPeer = PeerGroup.peerFromChannelFuture(channelFuture);
                        }
                    }).start();
                }
                AsyncUpdatePeer(peer, DataStore.PeerState.PEER_DISCONNECTED);
            }
            
            @Override
            public Message onPreMessageReceived(Peer peer, Message m) {
                if (m instanceof AddressMessage) {
                    if (peer != localPeer)
                        PeerProvidedAddressMessage(peer);
                    AsyncAddUntestedNodes(((AddressMessage)m).getAddresses());
                    return null;
                }
                if (peer == localPeer) {
                    if (m instanceof Block) {
                        synchronized(exitableLock) {
                            exitableSemaphore++;
                        }
                    }
                    return m;
                }
                if (m instanceof Block || m instanceof InventoryMessage) {
                    try {
                        ChannelFutureAndProgress peerState;
                        synchronized(peerToChannelMap) {
                            peerState = peerToChannelMap.get(peer);
                        }
                        if (peerState == null)
                            return null;
                        if (m instanceof InventoryMessage && !peerState.hasAskedForBlocks) {
                            for(InventoryItem inv : ((InventoryMessage)m).getItems()) {
                                if (inv.type == InventoryItem.Type.Block && inv.hash.equals(peerState.targetHash)) {
                                    GetDataMessage getdata = new GetDataMessage(params);
                                    getdata.addItem(inv);
                                    peer.sendMessage(getdata);
                                    peerState.hasAskedForBlocks = true;
                                    break;
                                }
                            }
                        } else if (m instanceof Block && ((Block)m).getHash().equals(peerState.targetHash)) {
                            PeerPassedBlockDownloadVerification(peer);
                        }
                    } catch (IOException e) {
                        AsyncUpdatePeer(peer, DataStore.PeerState.PEER_DISCONNECTED);
                    }
                    return null;
                }
                // TODO: Verify that nodes send transactions/alerts properly
                if (m instanceof Transaction || m instanceof AlertMessage)
                    return null;
                return m;
            }
            
            @Override
            public void onBlocksDownloaded(Peer peer, Block block, int blocksLeft) {
                try {
                    store.putHashAtHeight(blockStore.get(block.getHash()).getHeight(), block.getHash());
                } catch (BlockStoreException e) {
                    Dnsseed.ErrorExit(e);
                }
                synchronized(exitableLock) {
                    exitableSemaphore--;
                    exitableLock.notifyAll();
                }
                if (blocksLeft <= 0)
                    StartScan(peer);
            }
        });
    }
    
    private static void StartScan(final Peer localPeer) {
        synchronized(scanLock) {
            if (scanable)
                return;
            scanable = true;
            scanLock.notifyAll();
        }
        
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        localPeer.sendMessage(new GetAddrMessage(params));
                    } catch (IOException e) {
                        ErrorExit(e);
                    }
                    DnsDiscovery discovery = new DnsDiscovery(params);
                    try {
                        for (InetSocketAddress addr : discovery.getPeers(10, TimeUnit.SECONDS)) {
                            store.addUpdateNode(addr, DataStore.PeerState.UNTESTED);
                        }
                    } catch (PeerDiscoveryException e) { }
                    try {
                        Thread.sleep(60 * 1000);
                    } catch (InterruptedException e) { ErrorExit(e); }
                }
            }
        }).start();
    }
    
    static ScheduledThreadPoolExecutor nodeTimeoutExecutor = new ScheduledThreadPoolExecutor(1);
    private static void ScanHost(InetSocketAddress address) {
        synchronized(scanLock) {
            while (!scanable)
                try {
                    scanLock.wait();
                } catch (InterruptedException e) {
                    ErrorExit(e);
                }
        }
        
        ChannelFuture channelFuture = peerGroup.connectTo(address);
        final Peer peer = PeerGroup.peerFromChannelFuture(channelFuture);
        synchronized(peerToChannelMap) {
            peerToChannelMap.put(peer, new ChannelFutureAndProgress(channelFuture));
        }
        
        synchronized (store.totalRunTimeoutLock) {
            nodeTimeoutExecutor.schedule(new Runnable() {
                public void run() {
                    AsyncUpdatePeer(peer, null);
                }
            }, store.totalRunTimeout, TimeUnit.SECONDS);
        }
    }
    
    private static void PeerPassedBlockDownloadVerification(Peer peer) {
        ChannelFutureAndProgress peerState;
        synchronized(peerToChannelMap) {
            peerState = peerToChannelMap.get(peer);
            if (peerState != null) {
                peerState.hasPassedBlockDownloadTest = true;
                if (peerState.hasReceivedAddressMessage && !peerState.hasPassed) {
                    AsyncUpdatePeer(peer, DataStore.PeerState.GOOD);
                    peerState.hasPassed = true;
                }
            }
        }
    }
    
    private static void PeerProvidedAddressMessage(Peer peer) {
        ChannelFutureAndProgress peerState;
        synchronized(peerToChannelMap) {
            peerState = peerToChannelMap.get(peer);
            if (peerState != null) {
                peerState.hasReceivedAddressMessage = true;
                if (peerState.hasPassedBlockDownloadTest && !peerState.hasPassed) {
                    AsyncUpdatePeer(peer, DataStore.PeerState.GOOD);
                    peerState.hasPassed = true;
                }
            }
        }
    }
    
    static ExecutorService disconnectPeerExecutor = Executors.newFixedThreadPool(1);
    private static void AsyncUpdatePeer(final Peer peer, final DataStore.PeerState newState) {
        synchronized (disconnectPeerExecutor) {
            disconnectPeerExecutor.submit(new Runnable() {
                public void run() {
                    final ChannelFutureAndProgress peerState;
                    synchronized (peerToChannelMap) {
                        peerState = peerToChannelMap.remove(peer);
                        peerToChannelMap.notifyAll();
                    }
                    if (peerState != null) {
                        if (newState != null)
                            peerState.timeoutState = newState;
                        if (peerState.timeoutState != DataStore.PeerState.PEER_DISCONNECTED)
                            peerState.channel.getChannel().close();
                        store.addUpdateNode(
                                peer.getAddress().toSocketAddress(),
                                peerState.timeoutState);
                    }
                }
            });
        }
    }
    
    static ExecutorService addUpdateNodeExecutor = Executors.newFixedThreadPool(1);
    private static void AsyncAddUntestedNodes(final List<PeerAddress> addresses) {
        synchronized (addUpdateNodeExecutor) {
            addUpdateNodeExecutor.submit(new Runnable() {
                public void run() {
                    for (PeerAddress address : addresses)
                        store.addUpdateNode(address.toSocketAddress(),
                                DataStore.PeerState.UNTESTED);
                }
            });
        }
    }
    
    static int logFileCounter = 0;
    public static void LogLine(String line) {
        synchronized(logList) {
            logList.addLast(line);
            logList.removeFirst();
            try {
                logFileStream.write(line.getBytes());
                logFileStream.write('\n');
                if (logFileCounter++ % 10 == 0)
                    logFileStream.flush();
            } catch (IOException e) {
                ErrorExit(e);
            }
        }
    }
}
