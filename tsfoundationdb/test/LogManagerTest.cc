#include "spdlog/spdlog.h"
#include "LogManager.h"
#include "DataService.h"
#include "RpcMessage.pb.h"
#include "TestFramework.h"


int main() {
    spdlog::set_level(spdlog::level::debug);
    // init the log manager
    LogManager log_manager;
    // test write to log
    Message::MessageEntry *entry = new Message::MessageEntry();
    std::vector<std::string> timestamps{"1676968038", "1676968048", "1676968058"};
    std::vector<int> carids{1, 2, 3, 4};
    int x = 10344450;
    int y = 11563330;
    int v_x = 10333300;
    int v_y = 10998600;
    int v_r = 10776500;
    int direction = 87011110;
    std::string img = "12449226384";
    spdlog::debug("[LogManagerTest] begin test");
    for (const auto &time : timestamps) {
        for (const auto &id : carids) {
            entry->set_timestamp(time);
            entry->set_carid(id);
            entry->set_x(x);
            entry->set_y(y);
            entry->set_v_x(v_x);
            entry->set_v_y(v_y);
            entry->set_v_r(v_r);
            entry->set_direction(direction);
            entry->set_img(img);

            std::string data = DataService::SerializeMessage(entry);
            spdlog::debug("[LogManagerTest] serialize message success, the data: {}", data);
            log_manager.WriteToLog(data);
            spdlog::debug("[LogManagerTest] write to log success");
        }
    }

    // test restore the data
    std::vector<std::string> restore_data = log_manager.RestoreLog();
    spdlog::debug("[LogManagerTest] the restore count: {}", restore_data.size());
    int i = 0;
    for (const auto &data : restore_data) {
        MessageEntry entry = DataService::DeserializeMessage(data);
        int time_index = i / carids.size();
        int car_id_index = i % carids.size();

        ASSERT(entry.timestamp() == timestamps[time_index], "the timestamp not match");
        ASSERT(entry.carid() == carids[car_id_index], "the car id not match");
        ASSERT(entry.x() == 10344450, "the x not match");
        ASSERT(entry.y() == 11563330, "the y not match");
        ASSERT(entry.v_x() == 10333300, "the y not match");
        ASSERT(entry.v_y() == 10998600, "the y not match");
        ASSERT(entry.v_r() == 10776500, "the y not match");
        ASSERT(entry.direction() == 87011110, "the y not match");
        ASSERT(entry.img() == "12449226384", "the y not match");
    }

    return 0;
}

