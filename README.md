# dflake
Snowflake ID implementation for the D language.

```d
import snowflake;
auto gen = SnowflakeGenerator(123);
long id = gen.nextId();
```