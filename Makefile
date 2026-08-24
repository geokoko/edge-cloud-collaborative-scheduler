CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -Wpedantic

scheduler: main.cpp
	@size=$$(wc -c < $<); test $$size -le 65535 || { echo "$< has $$size bytes (max 65535)"; exit 1; }
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 $< -o $@

.PHONY: test clean
test: scheduler
	python3 tests/test_scheduler.py ./scheduler
	python3 tests/test_simulator.py ./scheduler

clean:
	$(RM) scheduler
