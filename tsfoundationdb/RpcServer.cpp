#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "protodata/RpcMessage.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using Message::TSDB;
using Message::MessageEntry;
using Message::Reply;
using Message::MessageInfo;

class TSDBServiceImpl final: public TSDB::Service {
public:

  Status TsSet(ServerContext* context, const MessageEntry* request, Reply* response) override {
    std::string timestamp = request->timestamp();
    uint32_t carid = request->carid();
    uint32_t x = request->x();
    uint32_t y = request->y();
    uint32_t v_x = request->v_x();
    uint32_t v_y = request->v_y();
    uint32_t v_r = request->v_r();
    std::string img = request->img();
    std::cout << timestamp << " " << carid << " " << x << " " << y << " " << v_x << " " << v_y << " " << v_r << img << std::endl;
    response->set_ret(true);
    return Status::OK;
  } 

  Status TsGet(ServerContext* context, const MessageInfo* request, MessageEntry* response) override {
    std::string timestamp = request->timestamp();
    uint32_t carid = request->carid();
    response->set_timestamp(timestamp);
    response->set_carid(carid);
    return Status::OK;
  }
};



int main(int argc, char** argv) {
  std::string server_address("0.0.0.0:5000");
  /* 定义重写的服务类 */
  TSDBServiceImpl service;

  /* 创建工厂类 */
  ServerBuilder builder;
  /* 监听端口和地址 */
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 5000);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  /* 注册服务 */
  builder.RegisterService(&service);
  /** 创建和启动一个RPC服务器*/
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  /* 进入服务事件循环 */
  server->Wait();
  return 0;
}
