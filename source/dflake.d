/** 
 * This module contains the entire DFlake library, which is mostly centered
 * around the SnowflakeGenerator.
 */
module dflake;

import core.sync.mutex : Mutex;

/** 
 * The Snowflake generator is a thread-safe generator of unique "snowflake"
 * ids, which guarantees that ids it generates will always be unique and
 * always increasing in value, provided that there are no two generators with
 * the same node id at the same time.
 *
 * Here's a simple example of how to generate ids:
 * ```d
 * auto gen = SnowflakeGenerator(1);
 * long id = gen.nextId;
 * ```
 *
 * For background info on this type of id, please see [Twitter's original blog
 * post on the topic](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake).
 *
 * Authors: Andrew Lalis, andrewlalisofficial@gmail.com
 */
struct SnowflakeGenerator {
    // Standard definitions of the structure of a snowflake id.
    public static const UNUSED_BITS = 1;
    public static const EPOCH_BITS = 41;
    public static const NODE_ID_BITS = 10;
    public static const SEQUENCE_BITS = 12;

    public static const long MAX_NODE_ID = (1L << NODE_ID_BITS) - 1;
    public static const long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;

    // Default epoch in case one isn't given. (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
    public static const long DEFAULT_CUSTOM_EPOCH = 1_420_070_400_000L;

    /** 
     * The node id for this generator.
     */
    private const long nodeId = 0;

    /** 
     * The epoch offset to use for this generator, in milliseconds since the
     * unix epoch.
     */
    private const long epoch = DEFAULT_CUSTOM_EPOCH;

    /** 
     * Tracks the last timestamp at which an id was issued, in milliseconds
     * since the epoch.
     */
    private long lastTimestamp = -1L;

    /** 
     * Tracks the last sequence value we used. If multiple ids are needed
     * within the span of 1 millisecond, this sequence is incremented for each
     * id.
     */
    private long sequence = 0L;

    /** 
     * A mutex to ensure that ids are generated sequentially, even if `nextId`
     * is called from multiple threads.
     */
    private shared Mutex mutex;

    /** 
     * Constructs the generator using a given node id and epoch offset.
     * Params:
     *   nodeId = The node id.
     *   epoch = The epoch offset, in milliseconds since 01-01-1970
     */
    this(long nodeId, long epoch) {
        assert(nodeId >= 0 && nodeId < MAX_NODE_ID);
        assert(epoch >= 0 && epoch < timestampMillis(0));
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.mutex = new shared Mutex();
    }

    /** 
     * Constructs the generator using a default epoch offset.
     * Params:
     *   nodeId = The node id.
     */
    this(long nodeId) {
        this(nodeId, DEFAULT_CUSTOM_EPOCH);
    }

    /** 
     * Generates a new unique id. Calls to this method are synchronized such
     * that ids from this generator are generated sequentially.
     * Returns: The id.
     */
    public long nextId() {
        // Aquire a lock so that only one id can be generated at once by a single generator.
        this.mutex.lock_nothrow();
        scope(exit) this.mutex.unlock_nothrow(); // Unlock when we're done.

        long currentTimestamp = timestampMillis(this.epoch);
        assert(currentTimestamp >= this.lastTimestamp);
        if (currentTimestamp == this.lastTimestamp) {
            this.sequence = (this.sequence + 1) & MAX_SEQUENCE;
            if (this.sequence == 0) {
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            this.sequence = 0;
        }

        this.lastTimestamp = currentTimestamp;
        long id = currentTimestamp << (NODE_ID_BITS + SEQUENCE_BITS)
            | (nodeId << SEQUENCE_BITS)
            | this.sequence;
        return id;
    }

    import std.typecons : Tuple, tuple;

    /** 
     * An alias for a tuple containing the basic information of a snowflake id.
     */
    alias SnowflakeIdInfo = Tuple!(long, "timestamp", long, "nodeId", long, "sequence");

    /** 
     * Parses a snowflake id into its constituent parts.
     * Params:
     *   id = The id to parse.
     * Returns: A tuple containing the timestamp, node id, and sequence value
     * of the snowflake id.
     */
    public SnowflakeIdInfo parseId(long id) {
        long maskNodeId = ((1L << NODE_ID_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;

        long timestamp = (id >> (NODE_ID_BITS + SEQUENCE_BITS)) + this.epoch;
        long nodeIdLocal = (id & maskNodeId) >> SEQUENCE_BITS;
        long sequenceLocal = id & maskSequence;
        return tuple!("timestamp", "nodeId", "sequence")(timestamp, nodeIdLocal, sequenceLocal);
    }

    /** 
     * Gets the current timestamp in milliseconds.
     * Params:
     *   offset = The offset to subtract from unix time.
     * Returns: The milliseconds that have elapsed since the given offset from
     * unix time.
     */
    private static long timestampMillis(long offset) {
        import std.datetime : Clock, SysTime, DateTime, UTC, Duration;
        SysTime now = Clock.currStdTime();
        SysTime unixEpoch = SysTime(DateTime(1970, 1, 1), UTC());
        Duration diff = now - unixEpoch;
        return diff.total!"msecs" - offset;
    }

    /** 
     * Performs a busy-wait until a single millisecond has elapsed since the
     * given timestamp.
     * Params:
     *   currentTimestamp = The current timestamp.
     * Returns: The next timestamp, once we reach it.
     */
    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestampMillis(this.epoch);
        }
        return currentTimestamp;
    }
}

// Tests for the correctness of the generator.
unittest {
    // Check that each next id is unique.
    auto gen = SnowflakeGenerator(123);
    const iterations = 5000;

    long[iterations] ids;
    foreach (i; 0 .. iterations) {
        ids[i] = gen.nextId;
    }

    foreach (i; 0 .. ids.length) {
        foreach (j; i + 1 .. ids.length) {
            assert(ids[i] != ids[j], "Ids generated by the same generator should be unique.");
            assert(ids[i] < ids[j], "Ids should be increasing in value over time.");
        }
    }

    // Check that ids generated from separate threads are unique.
    import core.thread : Thread;
    import std.format;
    import std.stdio;

    const iterationsPerThread = 1000;
    const threadCount = 50;
    const multiIdsSize = iterationsPerThread * threadCount;
    long[] multiIds = new long[multiIdsSize];
    Thread[threadCount] threads;
    auto gen2 = SnowflakeGenerator(345);

    class GenThread : Thread {
        private SnowflakeGenerator* gen;
        private const int startIdx;
        private const int endIdx;
        private long[] ids;
        this(SnowflakeGenerator* gen, int startIdx, int endIdx, ref long[] ids) {
            super(&run);
            this.gen = gen;
            this.startIdx = startIdx;
            this.endIdx = endIdx;
            this.ids = ids;
        }
        private void run() {
            foreach (i; startIdx .. endIdx) {
                ids[i] = this.gen.nextId();
            }
        }
    }

    writefln!"Generating %d ids using %d threads"(multiIdsSize, threadCount);
    foreach (i; 0 .. threadCount) {
        int start = i * iterationsPerThread;
        int end = (i + 1) * iterationsPerThread;
        threads[i] = new GenThread(&gen2, start, end, multiIds);
        threads[i].start();
    }

    foreach (t; threads) t.join();
    writeln("Finished generating ids.");

    writeln("Checking for duplicates in ids generated by multiple threads.");
    foreach (i; 0 .. multiIds.length) {
        foreach (j; i + 1 .. multiIds.length) {
            assert(multiIds[i] != multiIds[j], format!"Found duplicate at indexes %d and %d: %d"(i, j, multiIds[j]));
        }
    }
}

// Separate test for performance testing.
unittest {
    import std.stdio;
    import std.datetime.stopwatch;

    auto sw = StopWatch(AutoStart.no);

    void genIdsTest() {
        SnowflakeGenerator gen = SnowflakeGenerator(123);
        int iterationCount = 1_000_000;
        writefln!"Measuring the time taken to generate %d ids..."(iterationCount);
        sw.start();
        for (int i = 0; i < iterationCount; i++) {
            gen.nextId();
        }
        sw.stop();
        long ms = sw.peek().total!"msecs";
        double nsPerId = sw.peek().total!"nsecs" / cast(double) iterationCount;
        double idsPerSecond = 1_000_000_000.0 / nsPerId;
        writefln!"Generated %d ids in %d ms, or %.6f ns per id, or %.0f ids per second."(iterationCount, ms, nsPerId, idsPerSecond);
        sw.reset();
    }

    genIdsTest();
}