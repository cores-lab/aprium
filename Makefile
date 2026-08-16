CC = gcc
CFLAGS = -std=c23 -Wall -Wextra -MMD -O3 -mclflushopt -mclwb -mavx512f -flto -D_GNU_SOURCE
LDFLAGS = -lm -lpthread

SRC_FILES = main.c cli.c generate.c join.c mem.c cxl.c

SRC_DIR = src
BIN_DIR = bin
OBJ_DIR = obj

SRC = $(patsubst %, $(SRC_DIR)/%, $(SRC_FILES))
OBJ = $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(SRC))
TARGET = $(BIN_DIR)/aprium
DEP = $(OBJ:.o=.d)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJ) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR) $(OBJ_DIR):
	mkdir -p $@

clean:
	rm -r $(BIN_DIR) $(OBJ_DIR)

-include $(DEP)