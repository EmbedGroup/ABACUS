## ABACUS

  **DAG-based blockchain** systems have been deployed to enable trustworthy peer-to-peer transactions for **IoT** devices. **Unique address checking**, as a key part of transaction generation for privacy and security protection in DAG-based blockchain systems, incurs big latency overhead and degrades system throughput.

  We propose a Bloom-filter-based approach,called ABACUS to optimize the unique address checking process. In ABACUS, we partition the large address space into multiple small subspaces, and apply one Bloom filter to perform uniqueness checking for all addresses in a subspace.Specifically, we propose a two-level address space mechanism so as to strike a balance between the checking efficiency and the memory/storage space overhead of the Bloom filter design.A bucket-based scalable Bloom filter design is proposed to address the growth of used addresses and provide the checking latency guarantee with efficient I/O access through storing all sub-Bloom-filters together in one bucket. To further reduce
disk I/Os, ABACUS incorporates an in-memory write buffer and a read-only cache.

  We implemented ABACUS into IOTA, one of the most widely used DAG-based blockchain systems, and conducted a series of experiments on a private IOTA system. The experimental results show that ABACUS can significantly reduce the transaction generation time by up to four orders of magnitude while achieving up to 3X boost on the system throughput, compared with the original design.

## IOTA-Benchmark-Tool
  A benchmark tool for IOTA blockchain. The automatic transaction initiator is implemented with multi-threading NodeJS, utilizing APIs provided by IOTA foundation. Each initiator will automatically send transactions with pre-configured sender’s seed and receiver’s address in config.json.

  The monitor is implemented with Golang for high performance.

  Utilizing these tools, we could exhibit some unique characteristics of IOTA from several aspects such as performance, security, etc.
 
