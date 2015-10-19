package com.mattcorallo.bitcoinjdnsseed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.store.BlockStore;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;


interface FastSerializer {
    public void writeTo(ObjectOutputStream stream) throws IOException;
    public void readFrom(ObjectInputStream stream) throws IOException, ClassNotFoundException;
}

// Because we need to store persistent node objects to be removed efficiently later
class LinkedList<Type extends FastSerializer> {
    class Node {
        Node next = null, prev = null;
        Type object;
        Node(Type object) { this.object = object; }
    }
    transient Node head = null;
    transient Node tail = null;
    transient int count = 0;
    
    Node addToTail(Type object) {
        Node newNode = new Node(object);
        if (head == null && tail == null) {
            head = newNode;
            tail = newNode;
        } else if (head == null || tail == null) {
            Dnsseed.ErrorExit("Corrupted LinkedList");
        } else {
            tail.next = newNode;
            newNode.prev = tail;
            tail = newNode;
        }
        count++;
        return newNode;
    }
    
    void remove(Node node) {
        if (head == node && tail == node) {
            if (node.prev != null || node.next != null)
                Dnsseed.ErrorExit("Corrupted LinkedList");
            head = null;
            tail = null;
        }else if (head == node) {
            if (node.prev != null)
                Dnsseed.ErrorExit("Corrupted LinkedList");
            head = node.next;
            node.next.prev = node.prev;
        } else if (tail == node) {
            if (node.next != null)
                Dnsseed.ErrorExit("Corrupted LinkedList");
            tail = node.prev;
            node.prev.next = node.next;
        } else {
            if (node.prev == null || node.next == null)
                Dnsseed.ErrorExit("Corrupted LinkedList");
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
        count--;
    }
    
    int getSize() {
        return count;
    }
    
    Node getHead() {
        return head;
    }
    
    Node getTail() {
        return tail;
    }
    
    public void writeTo(ObjectOutputStream s) throws IOException {
        // Write out size
        s.writeInt(count);

        // Write out all elements in the proper order.
        for (Node tmp = head; tmp != null; tmp = tmp.next)
            tmp.object.writeTo(s);
    }
    
    public void readFrom(ObjectInputStream s, Class typeClass) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        // Read in size
        int size = s.readInt();

        // Read in all elements in the proper order.
        for (int i = 0; i < size; i++) {
            Type newObject = (Type)typeClass.newInstance();
            newObject.readFrom(s);
            addToTail(newObject);
        }
    }
}

class PeerAndLastUpdateTime implements FastSerializer {
    InetSocketAddress address = null;
    long lastUpdateTime = 0;
    long lastGoodTime;
    
    /**
     * Constructor
     * @param address
     * @param lastGoodTime if (-1) set to current time
     */
    PeerAndLastUpdateTime(InetSocketAddress address, long lastGoodTime) {
        this.address = address;
        this.lastUpdateTime = System.currentTimeMillis()/1000;
        if (lastGoodTime != -1)
            this.lastGoodTime = lastGoodTime;
        else
            this.lastGoodTime = this.lastUpdateTime;
    }
    
    // For deserialization
    PeerAndLastUpdateTime() {}
    
    public void writeTo(ObjectOutputStream stream) throws IOException {
        stream.writeObject(address);
        stream.writeLong(lastUpdateTime);
        stream.writeLong(lastGoodTime);
    }
    
    public void readFrom(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        this.address = (InetSocketAddress)stream.readObject();
        this.lastUpdateTime = stream.readLong();
        this.lastGoodTime = stream.readLong();
    }
}

public class MemoryDataStore extends DataStore {
    class PeerStateAndNode {
        PeerState state;
        LinkedList<PeerAndLastUpdateTime>.Node node;
        PeerStateAndNode(PeerState state, LinkedList<PeerAndLastUpdateTime>.Node node) {
            this.state = state;
            this.node = node;
        }
    }
    
    class UpdateState {
        public InetSocketAddress addr;
        public PeerState state;
        public long wasGoodCutoff;
        public UpdateState(InetSocketAddress addr, PeerState state, long wasGoodCutoff) {
            this.addr = addr;
            this.state = state;
            this.wasGoodCutoff = wasGoodCutoff;
        }
        // Make sure we are holding addressToStatusMapLock!
        public String runUpdate() {
            String logLine = null;
            PeerStateAndNode oldState = addressToStatusMap.get(addr);
            if (oldState == null || state != PeerState.UNTESTED) {
                if (state == PeerState.UNTESTED && badNodesFilter.mightContain(addr))
                    return null;
                if (state == PeerState.UNTESTED && statusToAddressesMap[state.ordinal()].getSize() > 10000)
                    return null;
                boolean print = false;
                if (oldState != null && oldState.state != PeerState.UNTESTED)
                    print = true;
                else if (state != PeerState.UNTESTED && state != PeerState.PEER_DISCONNECTED && state != PeerState.TIMEOUT)
                    print = true;
                else if (!addr.getAddress().toString().split("/")[0].equals("") && state != PeerState.UNTESTED)
                    print = true;
                if (oldState != null && oldState.state == PeerState.WAS_GOOD && (state == PeerState.TIMEOUT_DURING_REQUEST || state == PeerState.TIMEOUT || state == PeerState.PEER_DISCONNECTED))
                    print = false;
                if (print && (oldState == null || oldState.state != state))
                    logLine = (oldState != null ? ("Updated node " + addr.toString() + " state was " + oldState.state) :
                        ("Added node " + addr.toString())) + " new state is " + state.name();
                // Calculate last good time and check if we are WAS_GOOD
                long lastGoodTime = state == PeerState.GOOD ? -1 : (oldState != null ? oldState.node.object.lastGoodTime : 0);
                if (lastGoodTime > wasGoodCutoff)
                    state = PeerState.WAS_GOOD;
                LinkedList<PeerAndLastUpdateTime>.Node newNode = null;
                if (state != PeerState.PEER_DISCONNECTED && state != PeerState.TIMEOUT)
                    newNode = statusToAddressesMap[state.ordinal()].addToTail(new PeerAndLastUpdateTime(addr, lastGoodTime));
                else
                    addressToStatusMap.remove(addr);
                // Remove/Update
                if (oldState != null) {
                    statusToAddressesMap[oldState.state.ordinal()].remove(oldState.node);
                    oldState.state = state;
                    if (newNode != null)
                        oldState.node = newNode;
                    else
                        badNodesFilter.put(addr);
                } else if (newNode != null)
                    addressToStatusMap.put(addr, new PeerStateAndNode(state, newNode));
            }
            return logLine;
        }
    }
    Queue<UpdateState> queueStateUpdates = new java.util.LinkedList<UpdateState>();
        
    private String storageFile;
    
    public MemoryDataStore(String file, BlockStore store) {
        try {
            FileInputStream inStream = new FileInputStream(file + ".nodes");
            ObjectInputStream in = new ObjectInputStream(inStream);
            for (PeerState state : PeerState.values()) {
                statusToAddressesMap[state.ordinal()] = new LinkedList<PeerAndLastUpdateTime>();
                statusToAddressesMap[state.ordinal()].readFrom(in, PeerAndLastUpdateTime.class);
                LinkedList<PeerAndLastUpdateTime>.Node tmp = statusToAddressesMap[state.ordinal()].getHead();
                while (tmp != null) {
                    addressToStatusMap.put(tmp.object.address, new PeerStateAndNode(state, tmp));
                    tmp = tmp.next;
                }
            }
            in.close();
            inStream.close();
        } catch (FileNotFoundException e) {
            for (PeerState state : PeerState.values())
                statusToAddressesMap[state.ordinal()] = new LinkedList<PeerAndLastUpdateTime>();
        } catch (Exception e) {
            Dnsseed.ErrorExit(e);
        }
        
        try {
            FileInputStream inStream = new FileInputStream(file + ".settings");
            ObjectInputStream in = new ObjectInputStream(inStream);
            synchronized(retryTimesLock) {
                for (int i = 0; i < retryTimes.length; i++)
                    retryTimes[i] = in.readInt();
                ageOfLastSuccessToRetryAsGood = in.readInt();
            }
            synchronized(connectionsPerSecondLock) {
                connectionsPerSecond = in.readInt();
            }
            synchronized(totalRunTimeoutLock) {
                totalRunTimeout = in.readInt();
            }
            synchronized(minVersionLock) {
                minVersion = in.readInt();
            }
            in.close();
            inStream.close();
        } catch (FileNotFoundException e) {
        } catch (Exception e) {
            Dnsseed.ErrorExit(e);
        }
        
        storageFile = file;

        createFilter();

        //Kick off a thread to do the actual update processing
        new Thread() {
            public void run() {
                while (true) {
                    UpdateState update;
                    synchronized (queueStateUpdates) {
                        while (queueStateUpdates.isEmpty())
                            try { queueStateUpdates.wait(); } catch (InterruptedException e) { Dnsseed.ErrorExit(e); }
                        update = queueStateUpdates.poll();
                    }
                    addressToStatusMapLock.lock();
                    String line = update.runUpdate();
                    addressToStatusMapLock.unlock();
                    if (line != null)
                        Dnsseed.LogLine(line);
                }
            }
        }.start();
    }

    private void createFilter() {
        badNodesFilter = BloomFilter.create(new Funnel<InetSocketAddress>() {
            public void funnel(InetSocketAddress from, PrimitiveSink into) {
                into.putBytes(from.getAddress().getAddress());
            }
        }, 400000, 0.0001);
        badNodesFilterClearTime = System.currentTimeMillis();
    }
    
    Lock addressToStatusMapLock = new ReentrantLock();
    private HashMap<InetSocketAddress, PeerStateAndNode> addressToStatusMap = new HashMap<InetSocketAddress, PeerStateAndNode>();
    private LinkedList<PeerAndLastUpdateTime>[] statusToAddressesMap = new LinkedList[PeerState.values().length];

    private BloomFilter<InetSocketAddress> badNodesFilter;
    private long badNodesFilterClearTime;
    @Override
    public void addUpdateNode(InetSocketAddress addr, PeerState state) {
        if (state == PeerState.WAS_GOOD)
            Dnsseed.ErrorExit("addUpdateNode WAS_GOOD");
        long wasGoodCutoff;
        synchronized (retryTimesLock) {
            wasGoodCutoff = System.currentTimeMillis()/1000 - ageOfLastSuccessToRetryAsGood;
        }
        synchronized(queueStateUpdates) {
            queueStateUpdates.add(new UpdateState(addr, state, wasGoodCutoff));
            queueStateUpdates.notify();
        }
    }

    @Override
    public List<InetSocketAddress> getNodesToTest() {
        List<InetSocketAddress> resultsList = new java.util.LinkedList<InetSocketAddress>();
        if (addressToStatusMapLock.tryLock()) {
            for (PeerState state : PeerState.values()) {
                LinkedList<PeerAndLastUpdateTime>.Node temp = statusToAddressesMap[state.ordinal()].getHead();
                long targetMaxTime;
                synchronized (retryTimesLock) {
                    targetMaxTime = System.currentTimeMillis()/1000 - retryTimes[state.ordinal()];
                }
                while (temp != null) {
                    if (temp.object.lastUpdateTime >= targetMaxTime)
                        break;
                    resultsList.add(temp.object.address);
                    temp = temp.next;
                }
            }
            if (badNodesFilterClearTime < System.currentTimeMillis() - retryTimes[PeerState.TIMEOUT.ordinal()]*1000)
                createFilter();
            addressToStatusMapLock.unlock();
        }
        // We do significantly more work when testing nodes which return results,
        // so we shuffle the list around to distribute the GOOD/WAS_GOOD/UNTESTED nodes around
        Collections.shuffle(resultsList);
        return resultsList;
    }

    @Override
    public boolean shouldIgnoreAddr(InetSocketAddress addr) {
        return badNodesFilter.mightContain(addr);
    }

    @Override
    public List<InetAddress> getMostRecentGoodNodes(int numNodes, int port) {
        addressToStatusMapLock.lock();
        try {
            List<InetAddress> resultsList = new java.util.LinkedList<InetAddress>();
            LinkedList<PeerAndLastUpdateTime>.Node temp = statusToAddressesMap[PeerState.GOOD.ordinal()].getTail();
            while (temp != null) {
                if (resultsList.size() >= numNodes)
                    break;
                if (temp.object.address.getPort() == port)
                    resultsList.add(temp.object.address.getAddress());
                temp = temp.prev;
            }
            return resultsList;
        } finally {
            addressToStatusMapLock.unlock();
        }
    }
    
    @Override
    public String getStatus() {
        addressToStatusMapLock.lock();
        try {
            String states = "";
            int total = 0;
            for (PeerState state : PeerState.values()) {
                states += state.name() + ": ";
                for (int i = DataStore.PEER_STATE_MAX_LENGTH; i > state.name().length(); i--)
                    states += " ";
                int currentCount = statusToAddressesMap[state.ordinal()].getSize();
                total += currentCount;
                states += currentCount + "\n";
            }
            states += "Total: ";
            for (int i = DataStore.PEER_STATE_MAX_LENGTH; i > 5; i--)
                states += " ";
            states += total;
            return states;
        } finally {
            addressToStatusMapLock.unlock();
        }
    }

    public void saveNodesState() {
        try {
            FileOutputStream outStream = new FileOutputStream(storageFile + ".nodes.tmp");
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            addressToStatusMapLock.lock();
            try {
                for (LinkedList<PeerAndLastUpdateTime> list : statusToAddressesMap)
                    list.writeTo(out);
            } finally {
                addressToStatusMapLock.unlock();
            }
            out.close();
            outStream.close();
            new File(storageFile + ".nodes").delete();
            new File(storageFile + ".nodes.tmp").renameTo(new File(storageFile + ".nodes"));
        } catch (IOException e) {
            Dnsseed.ErrorExit(e);
        }
    }
    
    public void saveConfigState() {
        try {
            FileOutputStream outStream = new FileOutputStream(storageFile + ".settings.tmp");
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            synchronized(retryTimesLock) {
                for (int i : retryTimes)
                    out.writeInt(i);
                out.writeInt(ageOfLastSuccessToRetryAsGood);
            }
            synchronized(connectionsPerSecondLock) {
                out.writeInt(connectionsPerSecond);
            }
            synchronized(totalRunTimeoutLock) {
                out.writeInt(totalRunTimeout);
            }
            synchronized(minVersionLock) {
                out.writeInt(minVersion);
            }
            out.close();
            outStream.close();
            new File(storageFile + ".settings").delete();
            new File(storageFile + ".settings.tmp").renameTo(new File(storageFile + ".settings"));
        } catch (IOException e) {
            Dnsseed.ErrorExit(e);
        }
    }
}
