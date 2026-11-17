CC = cc
CFLAGS = -std=c11 -O2 -g -Wall -Wextra -Werror -Wpedantic

.PHONY: all test sanitize clean
all: goldb

goldb: src/goldb.c src/dict.c src/dict.h
	$(CC) $(CFLAGS) src/goldb.c src/dict.c -o $@

test: goldb
	python3 tests/test_server.py

sanitize: clean
	$(MAKE) CFLAGS='-std=c11 -O1 -g -Wall -Wextra -Werror -Wpedantic -fsanitize=address,undefined -fno-omit-frame-pointer' test

clean:
	rm -f goldb
