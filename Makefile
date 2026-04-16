CC = gcc
CFLAGS = -std=gnu23 -Wall -Wextra -MMD -O3 -mclflushopt -mclwb -mavx512f -flto -D_GNU_SOURCE
LDFLAGS = -lm
SRC_FILES = main.c cli.c generate.c join.c mem.c cxl.c
SRC_DIR = src
BIN_DIR = bin
SRC = $(patsubst %, $(SRC_DIR)/%, $(SRC_FILES))
OBJ = $(patsubst $(SRC_DIR)/%.c, $(BIN_DIR)/%.o, $(SRC))
TARGET = $(BIN_DIR)/prj
DEP = $(OBJ:.o=.d)

.PHONY: all clean $(BIN_DIR)

all: $(TARGET)

$(TARGET): $(OBJ) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BIN_DIR)/%.o: $(SRC_DIR)/%.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -c $< -o $@ $(LDFLAGS)

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -r $(BIN_DIR)

-include $(DEP)
