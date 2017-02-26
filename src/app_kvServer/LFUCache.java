package app_kvServer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public  class LFUCache implements Map<String, String>, CacheInterface{

    private final Map<String, CacheNode<String, String>> cache;
    private final LinkedHashSet[] frequencyList;
    private int lowestFrequency;
    private int maxFrequency;
    //
    private final int maxCacheSize;
    private final float evictionFactor;

    public LFUCache(int maxCacheSize, float evictionFactor) {
        if (evictionFactor <= 0 || evictionFactor >= 1) {
            throw new IllegalArgumentException("Eviction factor must be greater than 0 and lesser than or equal to 1");
        }
        this.cache = new HashMap<String, CacheNode<String, String>>(maxCacheSize);
        this.frequencyList = new LinkedHashSet[maxCacheSize];
        this.lowestFrequency = 0;
        this.maxFrequency = maxCacheSize - 1;
        this.maxCacheSize = maxCacheSize;
        this.evictionFactor = evictionFactor;
        initFrequencyList();
    }

    public String put(String k, String v) {
        String oldValue = null;
        CacheNode<String, String> currentNode = cache.get(k);
        if (currentNode == null) {
            if (cache.size() == maxCacheSize) {
                doEviction();
            }
            LinkedHashSet<CacheNode<String, String>> nodes = frequencyList[0];
            currentNode = new CacheNode(k, v, 0);
            nodes.add(currentNode);
            cache.put(k, currentNode);
            lowestFrequency = 0;
        } else {
            oldValue = currentNode.v;
            currentNode.v = v;
        }
        return oldValue;
    }


    public void putAll(Map<? extends String, ? extends String> map) {
        for (Map.Entry<? extends String, ? extends String> me : map.entrySet()) {
            put(me.getKey(), me.getValue());
        }
    }

    public String get(Object k) {
        CacheNode<String, String> currentNode = cache.get(k);
        if (currentNode != null) {
            int currentFrequency = currentNode.frequency;
            if (currentFrequency < maxFrequency) {
                int nextFrequency = currentFrequency + 1;
                LinkedHashSet<CacheNode<String, String>> currentNodes = frequencyList[currentFrequency];
                LinkedHashSet<CacheNode<String, String>> newNodes = frequencyList[nextFrequency];
                moveToNextFrequency(currentNode, nextFrequency, currentNodes, newNodes);
                cache.put((String) k, currentNode);
                if (lowestFrequency == currentFrequency && currentNodes.isEmpty()) {
                    lowestFrequency = nextFrequency;
                }
            } else {
                // Hybrid with LRU: put most recently accessed ahead of others:
                LinkedHashSet<CacheNode<String, String>> nodes = frequencyList[currentFrequency];
                nodes.remove(currentNode);
                nodes.add(currentNode);
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public String remove(Object k) {
        CacheNode<String, String> currentNode = cache.remove(k);
        if (currentNode != null) {
            LinkedHashSet<CacheNode<String, String>> nodes = frequencyList[currentNode.frequency];
            nodes.remove(currentNode);
            if (lowestFrequency == currentNode.frequency) {
                findNextLowestFrequency();
            }
            return currentNode.v;
        } else {
            return null;
        }
    }

    public int frequencyOf(String k) {
        CacheNode<String, String> node = cache.get(k);
        if (node != null) {
            return node.frequency + 1;
        } else {
            return 0;
        }
    }

    public void clear() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i].clear();
        }
        cache.clear();
        lowestFrequency = 0;
    }

    public Set<String> keySet() {
        return this.cache.keySet();
    }

    public Collection<String> values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Set<Entry<String, String>> entrySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public int size() {
        return cache.size();
    }

    public boolean isEmpty() {
        return this.cache.isEmpty();
    }

    public boolean containsKey(Object o) {
        return this.cache.containsKey(o);
    }

    public boolean containsValue(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    private void initFrequencyList() {
        for (int i = 0; i <= maxFrequency; i++) {
            frequencyList[i] = new LinkedHashSet<CacheNode<String, String>>();
        }
    }

    private void doEviction() {
        int currentlyDeleted = 0;
        float target = maxCacheSize * evictionFactor;
        while (currentlyDeleted < target) {
            LinkedHashSet<CacheNode<String, String>> nodes = frequencyList[lowestFrequency];
            if (nodes.isEmpty()) {
                throw new IllegalStateException("Lowest frequency constraint violated!");
            } else {
                Iterator<CacheNode<String, String>> it = nodes.iterator();
                while (it.hasNext() && currentlyDeleted++ < target) {
                    CacheNode<String, String> node = it.next();
                    it.remove();
                    cache.remove(node.k);
                }
                if (!it.hasNext()) {
                    findNextLowestFrequency();
                }
            }
        }
    }

    private void moveToNextFrequency(CacheNode<String, String> currentNode, int nextFrequency, LinkedHashSet<CacheNode<String, String>> currentNodes, LinkedHashSet<CacheNode<String, String>> newNodes) {
        currentNodes.remove(currentNode);
        newNodes.add(currentNode);
        currentNode.frequency = nextFrequency;
    }

    private void findNextLowestFrequency() {
        while (lowestFrequency <= maxFrequency && frequencyList[lowestFrequency].isEmpty()) {
            lowestFrequency++;
        }
        if (lowestFrequency > maxFrequency) {
            lowestFrequency = 0;
        }
    }

    private static class CacheNode<Key, Value> {

        public final Key k;
        public Value v;
        public int frequency;

        public CacheNode(Key k, Value v, int frequency) {
            this.k = k;
            this.v = v;
            this.frequency = frequency;
        }

    }


}