package com.mattcorallo.bitcoinjdnsseed;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.bitcoin.core.Sha256Hash;


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
    
    @Override
    public void writeTo(ObjectOutputStream stream) throws IOException {
        stream.writeObject(address);
        stream.writeLong(lastUpdateTime);
        stream.writeLong(lastGoodTime);
    }
    
    @Override
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
        
    private String storageFile;
    
    public MemoryDataStore(String file) {
        try {
            FileInputStream inStream = new FileInputStream(file);
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
            blockHashList = (ArrayList<Sha256Hash>) in.readObject();
            int numNulls = 0;
            for (Sha256Hash hash : blockHashList) {
                if (hash != null)
                    hashesStored++;
                else
                    numNulls++;
            }
            if (numNulls > 1 || blockHashList.get(0) != null)
                Dnsseed.ErrorExit((numNulls-1) + " null hash(es) in MemoryDataStore");
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
            in.close();
            inStream.close();
        } catch (FileNotFoundException e) {
            for (PeerState state : PeerState.values())
                statusToAddressesMap[state.ordinal()] = new LinkedList<PeerAndLastUpdateTime>();
        } catch (Exception e) {
            Dnsseed.ErrorExit(e);
        }
        blockHashList.ensureCapacity(300000);
        storageFile = file;
    }
    
    private HashMap<InetSocketAddress, PeerStateAndNode> addressToStatusMap = new HashMap<InetSocketAddress, PeerStateAndNode>();
    private LinkedList<PeerAndLastUpdateTime>[] statusToAddressesMap = new LinkedList[PeerState.values().length];

    @Override
    public void addUpdateNode(InetSocketAddress addr, PeerState state) {
        if (state == DataStore.PeerState.WAS_GOOD)
            Dnsseed.ErrorExit("addUpdateNode WAS_GOOD");
        long wasGoodCutoff;
        synchronized (retryTimesLock) {
            wasGoodCutoff = System.currentTimeMillis()/1000 - ageOfLastSuccessToRetryAsGood;
        }
        String logLine = null;
        synchronized (addressToStatusMap) {
            PeerStateAndNode oldState = addressToStatusMap.get(addr);
            if (oldState == null || state != PeerState.UNTESTED) {
                boolean print = false;
                if (oldState != null && oldState.state != PeerState.UNTESTED && oldState.state != PeerState.UNTESTABLE_ADDRESS)
                    print = true;
                else if (state != PeerState.UNTESTED && state != PeerState.PEER_DISCONNECTED && state != PeerState.TIMEOUT && state != PeerState.UNTESTABLE_ADDRESS)
                    print = true;
                else if (!addr.getAddress().toString().split("/")[0].equals("") && state != PeerState.UNTESTED && state != PeerState.UNTESTABLE_ADDRESS)
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
                LinkedList<PeerAndLastUpdateTime>.Node newNode = statusToAddressesMap[state.ordinal()].addToTail(new PeerAndLastUpdateTime(addr, lastGoodTime));
                // Remove/Update
                if (oldState != null) {
                    statusToAddressesMap[oldState.state.ordinal()].remove(oldState.node);
                    oldState.state = state;
                    oldState.node = newNode;
                } else
                    addressToStatusMap.put(addr, new PeerStateAndNode(state, newNode));
            }
        }
        if (logLine != null)
            Dnsseed.LogLine(logLine);
    }

    @Override
    public List<InetSocketAddress> getNodesToTest() {
        synchronized (addressToStatusMap) {
            List<InetSocketAddress> resultsList = new java.util.LinkedList<InetSocketAddress>();
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
            return resultsList;
        }
    }
    
    @Override
    public List<InetAddress> getMostRecentGoodNodes(int numNodes, int port) {
        synchronized (addressToStatusMap) {
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
        }
    }
    
    @Override
    public String getStatus() {
        synchronized (addressToStatusMap) {
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
        }
    }

    ArrayList<Sha256Hash> blockHashList = new ArrayList<Sha256Hash>(300000);
    int hashesStored = 0;
    @Override
    public int getMinBestHeight() {
        synchronized (blockHashList) {
            return blockHashList.size() - MIN_BLOCK_OFFSET;
        }
    }

    @Override
    public void putHashAtHeight(int height, Sha256Hash hash) {
        synchronized (blockHashList) {
            int origSize = blockHashList.size();
            for (int i = blockHashList.size(); i <= height; i++) {
                blockHashList.add(null);
            }
            blockHashList.set(height, hash);
            hashesStored += (blockHashList.size() != origSize) ? 1 : 0;
        }
    }

    @Override
    public Sha256Hash getHashAtHeight(int height) {
        synchronized (blockHashList) {
            return blockHashList.get(height);
        }
    }

    @Override
    public int getNumberOfHashesStored() {
        synchronized (blockHashList) {
            return hashesStored;
        }
    }
    
    public void saveState() {
        try {
            FileOutputStream outStream = new FileOutputStream(storageFile);
            ObjectOutputStream out = new ObjectOutputStream(outStream);
            synchronized (addressToStatusMap) {
                for (LinkedList<PeerAndLastUpdateTime> list : statusToAddressesMap)
                    list.writeTo(out);
            }
            synchronized (blockHashList) {
                out.writeObject(blockHashList);
            }
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
            out.close();
            outStream.close();
        } catch (Exception e) {
            Dnsseed.ErrorExit(e);
        }
    }
}
