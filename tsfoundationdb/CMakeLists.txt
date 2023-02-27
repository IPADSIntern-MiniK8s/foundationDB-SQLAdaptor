cmake_minimum_required(VERSION 3.5)

project(tsfoundationdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)


# find gRPC installation
# looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)


# find redis dependency
find_path(HIREDIS_HEADER hiredis)
# target_include_directories(test1 PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_LIB hiredis)
# target_link_libraries(test1  PRIVATE  ${HIREDIS_LIB})

include_directories (cache log metadata protodata test)
aux_source_directory (cache SRC_LIST1)
aux_source_directory (log SRC_LIST2)
aux_source_directory (metadata  SRC_LIST3)
aux_source_directory (protodata SRC_LIST4)
aux_source_directory (test SRC_LIST5)

# the main program
add_executable(tsdb_server RpcServer.cpp
${hw_proto_srcs}
${hw_grpc_srcs}
../protodata/RpcMessage.grpc.pb.cc
../protodata/RpcMessage.grpc.pb.h
../protodata/RpcMessage.pb.cc
../protodata/RpcMessage.pb.h)
target_link_libraries(tsdb_server ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF} ${HIREDIS_DEFAULT_LIBRARY})

# the redis test
add_executable(redis_test ./test/RedisUtilsTest.cc ${SRC_LIST1} ${SRC_LIST5})
target_include_directories(redis_test PUBLIC ${HIREDIS_HEADER})
target_link_libraries(redis_test ${HIREDIS_LIB})



