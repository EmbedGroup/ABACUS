package com.iota.iri.hotbf;

import java.util.HashMap;

public class LRUTable<K,V>{
    
    public class Node<K,V>{
        public Node<K,V> pre;
        public Node<K,V> next;
        public K key;
        public V value;
    }

    private Node<K,V> Head;
    private Node<K,V> Tail;
    private HashMap<K,Node<K,V>> map;

    public LRUTable(){
        map=new HashMap<>();
    }

    public void put(K key,V value){
        Node<K,V> e=map.get(key);
        if(e==null){
            e=new Node<>();
            e.key=key;
            e.value=value;
        }
        MoveToFirst(e);
        map.put(key,e);
    }
    
    public V Get(K key){
        Node<K,V> e=map.get(key);
        if(e==null) return null;
        //MoveToFirst(e);
        return e.value;
    }

    public void MoveToFirst(K key){
        Node<K,V> e=map.get(key);
        MoveToFirst(e);
    }
    public synchronized void MoveToFirst(Node<K,V> e){
        if(e==Head) return;
        if(e.pre != null) e.pre.next=e.next;
        if(e.next != null) e.next.pre=e.pre;
        if(e==Tail) Tail=Tail.pre;

        if(Head==null || Tail==null){
            Head=Tail=e;
            return;
        }

        e.next=Head;
        Head.pre=e;
        Head=e;
        e.pre=null;

    }

    public synchronized void RemoveTail(){
        if(Tail!=null){
            Tail=Tail.pre;
            if(Tail==null) Head=null;
            else Tail.next=null;
        }
    }
    public void PrintList(){
        Node<K,V> e=Head;
        while(e!=null){
            System.out.println(e.key+":"+e.value);
            e=e.next;
        }
    }
}