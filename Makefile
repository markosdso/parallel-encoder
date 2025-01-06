CC = gcc

CFLAGS = -g -Wall -Wextra -pedantic -O2
THREAD_FLAGS = -pthread

TARGET = parallelenc

SRC = parallelenc.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(THREAD_FLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)

.PHONY: all clean
