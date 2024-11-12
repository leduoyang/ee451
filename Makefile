CXX = g++
CXXFLAGS = -std=c++17

all: main serial

main: main.cpp
	$(CXX) -std=c++17 -o main main.cpp -lpthread

serial: serial.cpp
	$(CXX) -std=c++17 -o serial serial.cpp

clean:
	rm -f main serial

