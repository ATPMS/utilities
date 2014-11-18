ROUTE_BASE = ../../routing
CCFLAGS = -Ofast -I${ROUTE_BASE}/lib/boost.geometry/include -I${ROUTE_BASE} -g -frtti
CC = clang++

mmdump: mmdump.cpp ${ROUTE_BASE}/map_parser.cpp ${ROUTE_BASE}/utility.cpp
	${CC} -o mmdump ${CCFLAGS} -std=c++1y -L/usr/lib/x86_64-linux-gnu -L/usr/local/lib $^ -losmpbf -ljsoncpp -pthread -lz -lprotobuf-lite -lPocoFoundation -lfolly -lglog