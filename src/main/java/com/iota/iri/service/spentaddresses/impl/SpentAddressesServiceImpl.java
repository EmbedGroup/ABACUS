package com.iota.iri.service.spentaddresses.impl;

import com.iota.iri.BundleValidator;
import com.iota.iri.conf.MilestoneConfig;
import com.iota.iri.controllers.AddressViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hotbf.HotBF;
import com.iota.iri.metrics.ConsoleReporter;
import com.iota.iri.metrics.MetricRegistry;
import com.iota.iri.metrics.Timer;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.spentaddresses.SpentAddressesException;
import com.iota.iri.service.spentaddresses.SpentAddressesProvider;
import com.iota.iri.service.spentaddresses.SpentAddressesService;
import com.iota.iri.service.tipselection.TailFinder;
import com.iota.iri.service.tipselection.impl.TailFinderImpl;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.IotaUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.touk.throwing.ThrowingPredicate;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 
 * Implementation of <tt>SpentAddressesService</tt> that calculates and checks
 * spent addresses using the {@link Tangle}
 *
 */
public class SpentAddressesServiceImpl implements SpentAddressesService {

    private static final Logger log = LoggerFactory.getLogger(SpentAddressesServiceImpl.class);

    private Tangle tangle;

    static public MetricRegistry registry = new MetricRegistry();
    ConsoleReporter reporter = ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MICROSECONDS).build();
    static Timer timer;
    

    int wasAddressSpentFromCallTimers = 0;
    int getResultFromSpentAddressProvider = 0;
    int getResultFromBFs = 0;
    int getResultFromCheckedAddress = 0;

    private SnapshotProvider snapshotProvider;

    private SpentAddressesProvider spentAddressesProvider;

    private TailFinder tailFinder;

    private MilestoneConfig config;

    private BundleValidator bundleValidator;

    private final ExecutorService asyncSpentAddressesPersistor = IotaUtils
            .createNamedSingleThreadExecutor("Persist Spent Addresses Async");

//    private LeveledBloomFilter BFs;
    private HotBF HFBF;

    /**
     * Creates a Spent address service using the Tangler
     *
     * @param tangle                 Tangle object which is used to load models of
     *                               addresses
     * @param snapshotProvider       {@link SnapshotProvider} to find the genesis,
     *                               used to verify tails
     * @param spentAddressesProvider Provider for loading/saving addresses to a
     *                               database.
     * @return this instance
     */
    public SpentAddressesServiceImpl init(Tangle tangle, SnapshotProvider snapshotProvider,
            SpentAddressesProvider spentAddressesProvider, BundleValidator bundleValidator, MilestoneConfig config,
            HotBF bfs) {
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.spentAddressesProvider = spentAddressesProvider;
        this.bundleValidator = bundleValidator;
        this.tailFinder = new TailFinderImpl(tangle);
        this.config = config;
        this.HFBF = bfs;
        //reporter.start(60, TimeUnit.SECONDS,TimeUnit.MICROSECONDS);
        timer = registry.timer(MetricRegistry.name(SpentAddressesService.class, "wereAddressSpentFrom-ExeryMinute"));
        reporter.start(60, TimeUnit.SECONDS);

        return this;
    }

    @Override
    public boolean wasAddressSpentFrom(Hash addressHash) throws SpentAddressesException {
        return wasAddressSpentFrom(addressHash, getInitialUnspentAddresses());
    }

    @Override
    public void persistSpentAddresses(Collection<TransactionViewModel> transactions) throws SpentAddressesException {
        try {
            Collection<Hash> spentAddresses = transactions.stream()
                    .filter(ThrowingPredicate.unchecked(this::wasTransactionSpentFrom))
                    .map(TransactionViewModel::getAddressHash).collect(Collectors.toSet());

            spentAddressesProvider.saveAddressesBatch(spentAddresses);
        } catch (RuntimeException e) {
            throw new SpentAddressesException("Exception while persisting spent addresses", e);
        }
    }

    public void persistValidatedSpentAddressesAsync(Collection<TransactionViewModel> transactions) {
        asyncSpentAddressesPersistor.submit(() -> {
            try {
                List<Hash> spentAddresses = transactions.stream().filter(tx -> tx.value() < 0)
                        .map(TransactionViewModel::getAddressHash).collect(Collectors.toList());
                spentAddressesProvider.saveAddressesBatch(spentAddresses);
            } catch (Exception e) {
                log.error("Failed to persist spent-addresses... Counting on the Milestone Pruner to finish the job", e);
            }
        });
    }

    private boolean wasTransactionSpentFrom(TransactionViewModel tx) throws Exception {
        Optional<Hash> tailFromTx = tailFinder.findTailFromTx(tx);
        if (tailFromTx.isPresent() && tx.value() < 0) {
            // Transaction is confirmed
            if (tx.snapshotIndex() != 0) {
                return true;
            }

            // transaction is pending
            Hash tailHash = tailFromTx.get();
            return isBundleValid(tailHash);
        }

        return false;
    }

    private boolean isBundleValid(Hash tailHash) throws Exception {
        List<List<TransactionViewModel>> validation = bundleValidator.validate(tangle,
                snapshotProvider.getInitialSnapshot(), tailHash);
        return (CollectionUtils.isNotEmpty(validation) && validation.get(0).get(0).getValidity() == 1);
    }

    /**
     *
     * @param addressHash      the address in question
     * @param checkedAddresses known unspent addresses, used to skip calculations.
     *                         Must contain at least {@link Hash#NULL_HASH} and the
     *                         coordinator address.
     * @return {@code true} if address was spent from, else {@code false}
     * @throws SpentAddressesException
     * @see #wasAddressSpentFrom(Hash)
     */
    private boolean wasAddressSpentFrom(Hash addressHash, Collection<Hash> checkedAddresses)
            throws SpentAddressesException {
        // log.info("check address spent:" + addressHash);
        if (wasAddressSpentFromCallTimers++ == 100) {
            log.info("wasAddressSpentFrom Call 100:");
            log.info("get result from spentAddressProvider " + getResultFromSpentAddressProvider);
            log.info("get result from checkedAddress " + getResultFromCheckedAddress);
            log.info("get result from BFs " + getResultFromBFs);
            wasAddressSpentFromCallTimers = 0;
            getResultFromBFs = 0;
            getResultFromCheckedAddress = 0;
            getResultFromSpentAddressProvider = 0;
        }
        if (addressHash == null) {
            return false;
        }
        Timer.Context ctx = timer.time();

        try {
            if (spentAddressesProvider.containsAddress(addressHash)) {
                // log.info("checked spentaddressprovider return ture");
                getResultFromSpentAddressProvider++;
                return true;
            }
            // If address has already been checked this session, return false
            if (checkedAddresses.contains(addressHash)) {
                // log.info("checked checkedaddress return false");
                getResultFromCheckedAddress++;
                return false;
            }

            // 检查布隆过滤器，若是肯定不存在则返回false
//            if (!BFs.check(addressHash.toString())) {
//                // log.info("checked BFs,return false");
//                getResultFromBFs++;
//                return false;
//            }
            if (!HFBF.mayExists(addressHash.toString())) {
                // log.info("checked BFs,return false");
                getResultFromBFs++;
                return false;
            }
            try {
                Set<Hash> hashes = AddressViewModel.load(tangle, addressHash).getHashes();
                int setSizeLimit = 100_000;

                // If the hash set returned contains more than 100 000 entries, it likely will
                // not be a spent address.
                // To avoid unnecessary overhead while processing, the loop will return false
                if (hashes.size() > setSizeLimit) {
                    checkedAddresses.add(addressHash);
                    return false;
                }

                for (Hash hash : hashes) {
                    TransactionViewModel tx = TransactionViewModel.fromHash(tangle, hash);
                    // Check for spending transactions
                    if (wasTransactionSpentFrom(tx)) {
                        getResultFromBFs++;     //in case the address is truely spent
                        return true;
                    }
                }

            } catch (Exception e) {
                throw new SpentAddressesException(e);
            }

            checkedAddresses.add(addressHash);

            return false;
        } finally {
            ctx.stop();
        }
    }

    private Set<Hash> getInitialUnspentAddresses() {
        return Stream.of(Hash.NULL_HASH, config.getCoordinator()).collect(Collectors.toSet());
    }
}
