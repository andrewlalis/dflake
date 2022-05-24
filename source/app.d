import std.stdio;

import snowflake;

void main() {
	auto gen = SnowflakeGenerator(1L);
	int x = 0;
	while (x < 1000) {
		writeln(gen.nextId);
	}
}
