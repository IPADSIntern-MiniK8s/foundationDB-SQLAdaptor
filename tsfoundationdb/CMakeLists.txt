cmake_minimum_required(VERSION 3.5)

project(tsfoundationdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -pedantic -Wno-comment)
find_package(Threads REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
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

include_directories (cache log metadata protodata test storage service)
aux_source_directory (cache SRC_LIST1)
aux_source_directory (log SRC_LIST2)
aux_source_directory (metadata  SRC_LIST3)
aux_source_directory (protodata SRC_LIST4)
aux_source_directory (test SRC_LIST5)
aux_source_directory (storage SRC_LIST6)
aux_source_directory (service SRC_LIST7)

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
add_executable(redis_test ./test/RedisUtilsTest.cc ${SRC_LIST3} ${SRC_LIST4} ${SRC_LIST6} ${SRC_LIST7} ${SRC_LIST1})
target_include_directories(redis_test PUBLIC ${HIREDIS_HEADER})
target_link_libraries(redis_test ${HIREDIS_LIB} fdb_c m pthread rt ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# the storage test
add_executable(storage_test ./test/StorageTest.cc ${SRC_LIST6})
target_link_libraries(storage_test PRIVATE fdb_c m pthread rt)

# the protobuf serialize test
add_executable(serialize_test ./test/DataServiceTest.cc ${SRC_LIST7} ${SRC_LIST4})
target_link_libraries(serialize_test ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# the log test
add_executable(log_test ./test/LogManagerTest.cc ${SRC_LIST7} ${SRC_LIST4} ${SRC_LIST2})
target_link_libraries(log_test ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})