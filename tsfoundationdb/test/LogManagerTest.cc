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
        if (i >= 12) {
            break;
        }
        spdlog::debug("[LogManagerTest] the restore data: {}", data);
        MessageEntry *entry = DataService::DeserializeMessage(data);
        int time_index = i / carids.size();
        int car_id_index = i % carids.size();

        ASSERT(entry->timestamp() == timestamps[time_index], "the timestamp not match current: " + entry->timestamp() + "wanted: " + timestamps[time_index]);
        ASSERT(entry->carid() == carids[car_id_index], "the car id not match, current: " + std::to_string(entry->carid()) + "wanted: " + std::to_string(carids[car_id_index]));
        ASSERT(entry->x() == 10344450, "the x not match, current: " + std::to_string(entry->x()) + " " + std::to_string(i));
        ASSERT(entry->y() == 11563330, "the y not match, current: " + std::to_string(entry->y()) + " " + std::to_string(i));
        ASSERT(entry->v_x() == 10333300, "the v_x not match, current: " + std::to_string(entry->v_x()) + " " + std::to_string(i));
        ASSERT(entry->v_y() == 10998600, "the v_y not match, current: " + std::to_string(entry->v_y()) + " " + std::to_string(i));
        ASSERT(entry->v_r() == 10776500, "the v_r not match, current: " + std::to_string(entry->v_r()) + " " + std::to_string(i));
        ASSERT(entry->direction() == 87011110, "the direction not match, current: " + std::to_string(entry->v_r()) + " " + std::to_string(i));
        ASSERT(entry->img() == "12449226384", "the img not match, current: " + entry->img() + " " + std::to_string(i));
        ++i;
    }


    // test restore the data after a timestamp
    std::string timestamp = "2023 03 02 18:39:18";
    int prev_size = restore_data.size();
    restore_data = log_manager.RestoreLog(timestamp);
    ASSERT(restore_data.size() == prev_size - 12, "the entry count of restore log from a timestamp is not correct, wanted: " + std::to_string(prev_size - 12) + ", received: " + std::to_string(restore_data.size()));

    return 0;
}

