CC = gcc
CFLAGS = -std=gnu2x -Wall -Wextra -Werror -MMD
SRC_DIR = src
BIN_DIR = bin
SRC = $(wildcard $(SRC_DIR)/*.c)
OBJ = $(patsubst $(SRC_DIR)/%.c, $(BIN_DIR)/%.o, $(SRC))
TARGET = $(BIN_DIR)/prj
DEP = $(OBJ:.o=.d)

.PHONY: all clean $(BIN_DIR)

all: $(TARGET)

$(TARGET): $(OBJ) | $(BIN_DIR)
	$(CC) $(CFLAGS) -o $@ $^

$(BIN_DIR)/%.o: $(SRC_DIR)/%.c | $(BIN_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

clean:
	rm -r $(BIN_DIR)

-include $(DEP)
