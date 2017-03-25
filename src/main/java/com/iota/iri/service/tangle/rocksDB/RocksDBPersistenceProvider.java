package com.iota.iri.service.tangle.rocksDB;

import com.iota.iri.conf.Configuration;
import com.iota.iri.model.*;
import com.iota.iri.service.tangle.IPersistenceProvider;
import com.iota.iri.service.tangle.Serializer;
import com.iota.iri.service.viewModels.TransactionViewModel;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by paul on 3/2/17 for iri.
 */
public class RocksDBPersistenceProvider implements IPersistenceProvider {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RocksDBPersistenceProvider.class);
    private static int BLOOM_FILTER_RANGE = 1<<1;

    private String[] columnFamilyNames = new String[]{
            "transaction",
            "transactionValidity",
            "transactionType",
            "transactionArrivalTime",
            "transactionSolid",
            "transactionRating",
            "address",
            "bundle",
            "approovee",
            "tag",
            "tip",
    };

    boolean running;
    private ColumnFamilyHandle transactionHandle;
    private ColumnFamilyHandle transactionValidityHandle;
    private ColumnFamilyHandle transactionTypeHandle;
    private ColumnFamilyHandle transactionArrivalTimeHandle;
    private ColumnFamilyHandle transactionSolidHandle;
    private ColumnFamilyHandle transactionRatingHandle;
    private ColumnFamilyHandle addressHandle;
    private ColumnFamilyHandle bundleHandle;
    private ColumnFamilyHandle approoveeHandle;
    private ColumnFamilyHandle tagHandle;
    private ColumnFamilyHandle tipHandle;

    List<ColumnFamilyHandle> transactionGetList;

    private Map<Class<?>, ColumnFamilyHandle[]> classTreeMap = new HashMap<>();
    private Map<Class<?>, MyFunction<Object, Boolean>> saveMap = new HashMap<>();
    private Map<Class<?>, MyFunction<Object, Void>> deleteMap = new HashMap<>();
    private Map<Class<?>, MyFunction<Object, Boolean>> loadMap = new HashMap<>();
    private Map<Class<?>, MyFunction<Object, Boolean>> mayExistMap = new HashMap<>();
    private Map<Class<?>, ColumnFamilyHandle> countMap = new HashMap<>();

    RocksDB db;
    DBOptions options;
    private Random random;
    private Thread compactionThreadHandle;

    @Override
    public void init() throws Exception {
        initDB(
                Configuration.string(Configuration.DefaultConfSettings.DB_PATH),
                Configuration.string(Configuration.DefaultConfSettings.DB_LOG_PATH)
        );
        initClassTreeMap();
        initSaveMap();
        initLoadMap();
        initMayExistMap();
        initDeleteMap();
        initCountMap();
    }

    private void initCountMap() {
        countMap.put(Transaction.class, transactionHandle);
        countMap.put(Address.class, addressHandle);
        countMap.put(Bundle.class, bundleHandle);
        countMap.put(Approvee.class, approoveeHandle);
        countMap.put(Tag.class, tagHandle);
        countMap.put(Tip.class, tipHandle);
        /*
        countMap.put(AnalyzedFlag.class, analyzedFlagHandle);
        */
    }

    private void initDeleteMap() {
        deleteMap.put(Transaction.class, txObj -> {
            Transaction transaction = ((Transaction) txObj);
            byte[] key = transaction.hash.bytes();
            db.delete(transactionHandle, key);
            db.delete(transactionArrivalTimeHandle, key);
            db.delete(transactionTypeHandle, key);
            db.delete(transactionValidityHandle, key);
            db.delete(transactionSolidHandle, key);
            db.delete(transactionRatingHandle, key);
            return null;
        });
        deleteMap.put(Tip.class, txObj -> {
            db.delete(tipHandle, ((Tip) txObj).hash.bytes());
            return null;
        });
    }

    private void initMayExistMap() {
        mayExistMap.put(Transaction.class, txObj -> db.keyMayExist(transactionHandle, ((Transaction) txObj).hash.bytes(), new StringBuffer()));
        mayExistMap.put(Tip.class, txObj -> db.keyMayExist(tipHandle, ((Tip) txObj).hash.bytes(), new StringBuffer()));
    }

    private void initLoadMap() {
        loadMap.put(Transaction.class, getTransaction);
        loadMap.put(Address.class, getAddress);
        loadMap.put(Tag.class, getTag);
        loadMap.put(Bundle.class, getBundle);
        loadMap.put(Approvee.class, getApprovee);
    }

    private void initSaveMap() {
        saveMap.put(Transaction.class, saveTransaction);
        saveMap.put(Tip.class, saveTip);
    }

    private void initClassTreeMap() {
        classTreeMap.put(Address.class, new ColumnFamilyHandle[]{addressHandle});
        classTreeMap.put(Tip.class, new ColumnFamilyHandle[]{tipHandle});
        classTreeMap.put(Approvee.class, new ColumnFamilyHandle[]{approoveeHandle});
        classTreeMap.put(Bundle.class, new ColumnFamilyHandle[]{bundleHandle});
        classTreeMap.put(Tag.class, new ColumnFamilyHandle[]{tagHandle});
        classTreeMap.put(Transaction.class, new ColumnFamilyHandle[]{
                transactionHandle,
                transactionArrivalTimeHandle,
                transactionTypeHandle,
                transactionValidityHandle,
        });
    }

    @Override
    public void shutdown() {
        running = false;
        try {
            compactionThreadHandle.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (db != null) db.close();
        options.close();
    }

    private MyFunction<Object, Boolean> saveTransaction = (txObject -> {
        Transaction transaction = (Transaction) txObject;
        WriteBatch batch = new WriteBatch();
        byte[] key = transaction.hash.bytes();
        batch.put(transactionHandle, key, transaction.bytes);
        batch.put(transactionValidityHandle, key, Serializer.serialize(transaction.validity));
        batch.put(transactionTypeHandle, key, Serializer.serialize(transaction.type));
        batch.put(transactionArrivalTimeHandle, key, Serializer.serialize(transaction.arrivalTime));
        batch.put(transactionSolidHandle, key, Serializer.serialize(transaction.solid));
        batch.put(transactionRatingHandle, key, Serializer.serialize(transaction.rating));
        batch.merge(addressHandle, transaction.address.hash.bytes(), key);
        batch.merge(bundleHandle, transaction.bundle.hash.bytes(), key);
        batch.merge(approoveeHandle, transaction.trunk.hash.bytes(), key);
        batch.merge(approoveeHandle, transaction.branch.hash.bytes(), key);
        batch.merge(tagHandle, transaction.tag.value.bytes(), key);
        db.write(new WriteOptions(), batch);
        return true;
    });

    private MyFunction<Object, Boolean> saveTip = tipObj -> {
        db.put(tipHandle, ((Tip) tipObj).hash.bytes(), ((Tip) tipObj).status);
        return true;
    };

    @Override
    public boolean save(Object thing) throws Exception {
        return saveMap.get(thing.getClass()).apply(thing);
    }

    @Override
    public void delete(Object thing) throws Exception {
        deleteMap.get(thing.getClass()).apply(thing);
    }

    private Hash[] byteToHash(byte[] bytes, int size) {
        if(bytes == null) {
            return new Hash[0];
        }
        int i;
        Set<Hash> hashes = new TreeSet<>();
        for(i = size; i <= bytes.length; i += size + 1) {
            hashes.add(new Hash(Arrays.copyOfRange(bytes, i - size, i)));
        }
        return hashes.stream().toArray(Hash[]::new);
    }

    @Override
    public boolean exists(Class<?> model, Hash key) throws Exception {
        if(model == Transaction.class) {
            return db.get(transactionHandle, key.bytes()) != null;
        }
        throw new NotImplementedException("Mada mada exists shinai");
    }

    @Override
    public Object latest(Class<?> model) throws Exception {
        return null;
    }

    @Override
    public Object[] getKeys(Class<?> modelClass) throws Exception {
        if(modelClass == Transaction.class) {
            return getMissingTransactions();
        }
        List<byte[]> tips = new ArrayList<>();
        RocksIterator iterator = db.newIterator(tipHandle);
        for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            tips.add(iterator.key());
        }
        iterator.close();
        return tips.stream().map(Hash::new).toArray();
    }

    private Object[] getMissingTransactions() throws RocksDBException {
        List<byte[]> txToRequest = new ArrayList<>();
        RocksIterator iterator = db.newIterator(approoveeHandle);
        for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            if(db.get(transactionHandle, iterator.key()) == null)
                txToRequest.add(iterator.key());
        }
        iterator.close();
        return txToRequest.stream().map(Hash::new).toArray();
    }

    @Override
    public boolean get(Object model) throws Exception {
        return loadMap.get(model.getClass()).apply(model);
    }

    private MyFunction<Object, Boolean> getTransaction = (txObject) -> {
        Transaction transaction = ((Transaction) txObject);
        byte[] key = transaction.hash.bytes();
        transaction.bytes = db.get(transactionHandle, key);
        if(transaction.bytes == null) {
            transaction.type = TransactionViewModel.PREFILLED_SLOT;
            return false;
        } else if (transaction.bytes.length != TransactionViewModel.SIZE) {
            delete(transaction);
            transaction.type = TransactionViewModel.PREFILLED_SLOT;
            return false;
        }
        transaction.validity = Serializer.getInteger(db.get(transactionValidityHandle, key));
        transaction.type = Serializer.getInteger(db.get(transactionTypeHandle, key));
        transaction.arrivalTime = Serializer.getLong(db.get(transactionArrivalTimeHandle, key));
        transaction.solid =  db.get(transactionSolidHandle, key);
        transaction.rating = Serializer.getLong(db.get(transactionRatingHandle, key));
        return true;
    };

    private MyFunction<Object, Boolean> getAddress = (addrObject) -> {
        Address address = ((Address) addrObject);
        byte[] result = db.get(addressHandle, address.hash.bytes());
        if(result != null) {
            address.transactions = byteToHash(result, Hash.SIZE_IN_BYTES);
            return  true;
        } else {
            address.transactions = new Hash[0];
            return false;
        }
    };

    private MyFunction<Object, Boolean> getTag = tagObj -> {
        Tag tag = ((Tag) tagObj);
        byte[] result = db.get(tagHandle, tag.value.bytes());
        if(result != null) {
            tag.transactions = byteToHash(result, Hash.SIZE_IN_BYTES);
            return  true;
        } else {
            tag.transactions = new Hash[0];
            return false;
        }
    };

    private MyFunction<Object, Boolean> getBundle = bundleObj -> {
        Bundle bundle = ((Bundle) bundleObj);
        byte[] result = db.get(bundleHandle, bundle.hash.bytes());
        if(result != null) {
            bundle.transactions = byteToHash(result, Hash.SIZE_IN_BYTES);
            return true;
        }
        return false;
    };

    private MyFunction<Object, Boolean> getApprovee = approveeObj -> {
        Approvee approvee = ((Approvee) approveeObj);
        byte[] result = db.get(approoveeHandle, approvee.hash.bytes());
        if(result != null) {
            approvee.transactions = byteToHash(result, Hash.SIZE_IN_BYTES);
            return true;
        } else {
            approvee.transactions = new Hash[0];
            return false;
        }
    };

    @Override
    public boolean mayExist(Object model) throws Exception {
        return mayExistMap.get(model.getClass()).apply(model);
    }

    @Override
    public long count(Class<?> model) throws Exception {
        ColumnFamilyHandle handle = countMap.get(model);
        return db.getLongProperty(handle, "rocksdb.estimate-num-keys");
    }

    private void flushHandle(ColumnFamilyHandle handle) throws RocksDBException {
        //db.flush(new FlushOptions().setWaitForFlush(true), handle);
        List<byte[]> itemsToDelete = new ArrayList<>();
        RocksIterator iterator = db.newIterator(handle);
        for(iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
            itemsToDelete.add(iterator.key());
        }
        iterator.close();
        if(itemsToDelete.size() > 0) {
            log.info("Flushing flags. Amount to delete: " + itemsToDelete.size());
        }
        for(byte[] itemToDelete: itemsToDelete) {
            db.delete(handle, itemToDelete);
        }
    }



    @Override
    public boolean update(Object thing, String item) throws Exception {
        if(thing instanceof Transaction) {
            Transaction transaction = (Transaction) thing;
            byte[] key = transaction.hash.bytes();
            switch (item) {
                case "validity":
                    db.put(transactionValidityHandle, key, Serializer.serialize(transaction.validity));
                    break;
                case "type":
                    db.put(transactionValidityHandle, key, Serializer.serialize(transaction.type));
                    break;
                case "arrivalTime":
                    db.put(transactionValidityHandle, key, Serializer.serialize(transaction.arrivalTime));
                    break;
                case "solid":
                    db.put(transactionSolidHandle, key, Serializer.serialize(transaction.solid));
                    break;
                case "rating":
                    db.put(transactionRatingHandle, key, Serializer.serialize(transaction.rating));
                    break;
                default:
                    throw new NotImplementedException("Mada Sono Update ga dekinai yo");
            }
        } else {
            throw new NotImplementedException("Mada Sono Update ga dekinai yo");
        }
        return true;
    }


    void initDB(String path, String logPath) throws Exception {
        random = new Random();
        StringAppendOperator stringAppendOperator = new StringAppendOperator();
        RocksDB.loadLibrary();
        Thread.yield();
        BloomFilter bloomFilter = new BloomFilter(BLOOM_FILTER_RANGE);
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig().setFilter(bloomFilter);
        options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setDbLogDir(logPath);

        List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
        List<ColumnFamilyDescriptor> familyDescriptors = Arrays.stream(columnFamilyNames)
                .map(name -> new ColumnFamilyDescriptor(name.getBytes(),
                        new ColumnFamilyOptions()
                                .setMergeOperator(stringAppendOperator).setTableFormatConfig(blockBasedTableConfig))).collect(Collectors.toList());

        familyDescriptors.add(0, new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));

        //fillMissingColumns(familyDescriptors, familyHandles, path);

        db = RocksDB.open(options, path, familyDescriptors, familyHandles);
        db.enableFileDeletions(true);

        fillmodelColumnHandles(familyHandles);
    }

    private void fillMissingColumns(List<ColumnFamilyDescriptor> familyDescriptors, List<ColumnFamilyHandle> familyHandles, String path) throws Exception {
        List<ColumnFamilyDescriptor> columnFamilies = RocksDB.listColumnFamilies(new Options().setCreateIfMissing(true), path)
                .stream()
                .map(b -> new ColumnFamilyDescriptor(b, new ColumnFamilyOptions()))
                .collect(Collectors.toList());
        columnFamilies.add(0, familyDescriptors.get(0));
        List<ColumnFamilyDescriptor> missingFromDatabase = familyDescriptors.stream().filter(d -> columnFamilies.stream().filter(desc -> new String(desc.columnFamilyName()).equals(new String(d.columnFamilyName()))).toArray().length == 0).collect(Collectors.toList());
        List<ColumnFamilyDescriptor> missingFromDescription = columnFamilies.stream().filter(d -> familyDescriptors.stream().filter(desc -> new String(desc.columnFamilyName()).equals(new String(d.columnFamilyName()))).toArray().length == 0).collect(Collectors.toList());
        if (missingFromDatabase.size() != 0) {
            missingFromDatabase.remove(familyDescriptors.get(0));
            db = RocksDB.open(options, path, columnFamilies, familyHandles);
            for (ColumnFamilyDescriptor description : missingFromDatabase) {
                addColumnFamily(description.columnFamilyName(), db);
            }
            db.close();
        }
        if (missingFromDescription.size() != 0) {
            missingFromDescription.stream().forEach(familyDescriptors::add);
        }
        running = true;
        this.compactionThreadHandle = new Thread(() -> {
            long compationWaitTime = 5 * 60 * 1000;
            while(running) {
                try {
                    for(ColumnFamilyHandle handle: familyHandles) {
                        db.compactRange(handle);
                    }
                    Thread.sleep(compationWaitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void addColumnFamily(byte[] familyName, RocksDB db) throws RocksDBException {
        final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                new ColumnFamilyDescriptor(familyName,
                        new ColumnFamilyOptions()));
        assert (columnFamilyHandle != null);
    }

    private void fillmodelColumnHandles(List<ColumnFamilyHandle> familyHandles) throws Exception {
        int i = 0;
        transactionHandle = familyHandles.get(++i);
        transactionValidityHandle = familyHandles.get(++i);
        transactionTypeHandle = familyHandles.get(++i);
        transactionArrivalTimeHandle = familyHandles.get(++i);
        transactionSolidHandle = familyHandles.get(++i);
        transactionRatingHandle = familyHandles.get(++i);
        addressHandle = familyHandles.get(++i);
        bundleHandle = familyHandles.get(++i);
        approoveeHandle = familyHandles.get(++i);
        tagHandle = familyHandles.get(++i);
        tipHandle = familyHandles.get(++i);

        for(; ++i < familyHandles.size();) {
            db.dropColumnFamily(familyHandles.get(i));
        }

        updateTagDB();
        scanTxDeleteBaddies();
        updateSolidTx();

        this.compactionThreadHandle = new Thread(() -> {
            while(running) {
                try {
                    db.compactRange();
                    Thread.sleep(300 * 1000);
                } catch (Exception e) {
                    log.error("Compaction Error: " + e.getLocalizedMessage());
                }
            }
        }, "Compaction Thread");

        transactionGetList = new ArrayList<>();
        for(i = 1; i < 5; i ++) {
            transactionGetList.add(familyHandles.get(i));
        }
    }

    private void updateSolidTx() {
    }

    private void scanTxDeleteBaddies() throws Exception {
        RocksIterator iterator = db.newIterator(transactionHandle);
        List<byte[]> baddies = new ArrayList<>();
        WriteBatch batch = new WriteBatch();
        for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            if(iterator.value().length != TransactionViewModel.SIZE || Arrays.equals(iterator.value(), TransactionViewModel.NULL_TRANSACTION_BYTES)) {
                baddies.add(iterator.key());
            } else {
                batch.put(transactionSolidHandle, iterator.key(), new byte[]{0});
                batch.put(transactionRatingHandle, iterator.key(), Serializer.serialize((long)0));
            }
        }
        iterator.close();
        if(baddies.size() > 0) {
            log.info("Flushing corrupted transactions. Amount to delete: " + baddies.size());
        }
        for(byte[] baddie : baddies) {
            db.delete(transactionHandle, baddie);
        }
        db.write(new WriteOptions(), batch);
    }

    private void updateTagDB() throws RocksDBException {

        RocksIterator iterator = db.newIterator(tagHandle);
        byte[] key;
        List<byte[]> delList = new ArrayList<>();
        WriteBatch batch = new WriteBatch();
        for(iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            if(iterator.key().length < Hash.SIZE_IN_BYTES) {
                key = ArrayUtils.addAll(iterator.key(), Arrays.copyOf(TransactionViewModel.NULL_TRANSACTION_HASH_BYTES, Hash.SIZE_IN_BYTES - iterator.key().length));
                batch.put(tagHandle, key, iterator.value());
                delList.add(iterator.key());
            }
        }
        iterator.close();
        db.write(new WriteOptions(), batch);
        if(delList.size() > 0) {
            log.info("Flushing corrupted tag handles. Amount to delete: " + delList.size());
        }
        for(byte[] bytes: delList) {
            db.delete(tagHandle, bytes);
        }
    }

    @FunctionalInterface
    private interface MyFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
