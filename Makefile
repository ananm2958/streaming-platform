CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
LDFLAGS = -lstdc++fs
INCLUDES = \
	-Iinclude \
	-Iinclude/core \
	-Iinclude/config \
	-Iinclude/network \
	-Iinclude/protocol \
	-Iinclude/replication \
	-Iinclude/storage

SRC = \
	src/main.cpp \
	src/core/broker.cpp \
	src/config/broker_config.cpp \
	src/network/tcp_server.cpp \
	src/protocol/protocol.cpp \
	src/protocol/frame.cpp \
	src/replication/replication_manager.cpp \
	src/storage/storage_engine.cpp \
	src/storage/log.cpp \
	src/storage/log_segment.cpp

OBJ = $(SRC:.cpp=.o)
BROKER_OBJ = $(filter-out src/main.o, $(OBJ))

TARGET = broker
PROTOCOL_TEST = tests/protocol_test
INTEGRATION_TEST = tests/integration_test

.PHONY: all clean test

all: $(TARGET)

test: $(PROTOCOL_TEST) $(INTEGRATION_TEST)
	./$(PROTOCOL_TEST)
	./$(INTEGRATION_TEST)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^ $(LDFLAGS)

$(PROTOCOL_TEST): tests/protocol_test.o src/protocol/protocol.o src/protocol/frame.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^

$(INTEGRATION_TEST): tests/integration_test.o src/protocol/protocol.o src/protocol/frame.o
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^

src/%.o: src/%.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

tests/%.o: tests/%.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJ) tests/*.o $(TARGET) $(PROTOCOL_TEST) $(INTEGRATION_TEST)
