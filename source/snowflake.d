module snowflake;

const UNUSED_BITS = 1;
const EPOCH_BITS = 41;
const NODE_ID_BITS = 10;
const SEQUENCE_BITS = 12;

const long MAX_NODE_ID = (1L << NODE_ID_BITS) - 1;
const long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;

// Custom Epoch (January 1, 2015 Midnight UTC = 2015-01-01T00:00:00Z)
const long DEFAULT_CUSTOM_EPOCH = 1_420_070_400_000L;

/** 
 * The Snowflake generator is a thread-safe generator
 */
struct SnowflakeGenerator {
    import core.sync.mutex : Mutex;
    private Mutex idGenGuard;

    private const long nodeId;
    private const long epoch;

    private shared long lastTimestamp = -1L;
    private shared long sequence = 0L;

    this(long nodeId, long epoch) {
        this.nodeId = nodeId;
        this.epoch = epoch;
        this.idGenGuard = new Mutex();
    }

    this(long nodeId) {
        this(nodeId, DEFAULT_CUSTOM_EPOCH);
    }

    public long nextId() {
        this.idGenGuard.lock_nothrow();
        long currentTimestamp = timestampMillis(this.epoch);
        if (currentTimestamp < lastTimestamp) throw new Exception("Invalid clock!");
        if (currentTimestamp == this.lastTimestamp) {
            this.sequence = (this.sequence + 1) & MAX_SEQUENCE;
            if (this.sequence == 0) {
                currentTimestamp = waitNextMillis(currentTimestamp);
            }
        } else {
            sequence = 0;
        }

        this.lastTimestamp = currentTimestamp;
        long id = currentTimestamp << (NODE_ID_BITS + SEQUENCE_BITS)
            | (nodeId << SEQUENCE_BITS)
            | this.sequence;
        this.idGenGuard.unlock_nothrow();
        return id;
    }

    import std.typecons : Tuple, tuple;
    alias SnowflakeIdInfo = Tuple!(long, "timestamp", long, "nodeId", long, "sequence");

    public SnowflakeIdInfo parseId(long id) {
        long maskNodeId = ((1L << NODE_ID_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;

        long timestamp = (id >> (NODE_ID_BITS + SEQUENCE_BITS)) + this.epoch;
        long nodeIdLocal = (id & maskNodeId) >> SEQUENCE_BITS;
        long sequenceLocal = id & maskSequence;
        return tuple!("timestamp", "nodeId", "sequence")(timestamp, nodeIdLocal, sequenceLocal);
    }

    private static long timestampMillis(long offset) {
        import std.datetime.systime : Clock, SysTime;
        SysTime now = Clock.currStdTime();
        long hnSecs = now.stdTime();
        long ms = hnSecs / 10_000;
        return ms - offset;
    }

    private long waitNextMillis(long currentTimestamp) {
        while (currentTimestamp == lastTimestamp) {
            currentTimestamp = timestampMillis(this.epoch);
        }
        return currentTimestamp;
    }
}

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
            assert(ids[i] != ids[j]);
        }
    }

    // Check that ids generated from separate threads are unique.
    // TODO: Implement this once I make a @nogc lock mechanism.
}