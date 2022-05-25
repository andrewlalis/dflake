# dflake
Snowflake ID implementation for the D language, for generating increasing, unique ids, without the need for synchronization of distributed systems.

```d
import dflake;
auto gen = SnowflakeGenerator(123);
long id = gen.nextId();
```

### Performance
Each individual generator can produce ids in a sequential, thread-safe manner. You can clone this project and run `dub test` to run the test suite which includes some performance tests.

On my machine (AMD Ryzen Threadripper 2920X 12-Core Processor), I have benchmarked it at generating slightly more than 4,000,000 ids per second.

Inspired by a [Java implementation by callicoder](https://github.com/callicoder/java-snowflake).

For more information about the Snowflake ID concept, [read Twitter's blog post](https://blog.twitter.com/engineering/en_us/a/2010/announcing-snowflake).