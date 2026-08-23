CXX ?= g++
CXXFLAGS ?= -O2 -Wall -Wextra -Wpedantic

scheduler: main.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 $< -o $@

.PHONY: test clean
test: scheduler
	python3 tests/test_scheduler.py ./scheduler
	python3 tests/test_simulator.py ./scheduler

clean:
	$(RM) scheduler
