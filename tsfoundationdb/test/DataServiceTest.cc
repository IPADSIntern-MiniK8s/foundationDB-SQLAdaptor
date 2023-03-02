#include "../service/DataService.h"
#include "TestFramework.h"

int main() {
    MessageEntry input_entry;
    input_entry.set_carid(1);
    input_entry.set_timestamp("112233");
    input_entry.set_x(12);
    input_entry.set_y(34);
    input_entry.set_v_x(56);
    input_entry.set_v_y(78);
    input_entry.set_v_r(17);
    input_entry.set_direction(12345);

    std::string mid_result = DataService::SerializeMessage(&input_entry);
    MessageEntry *output_entry = DataService::DeserializeMessage(mid_result);

    ASSERT(input_entry.carid() == output_entry->carid(), "serialize/deserialize test fail");
    ASSERT(input_entry.timestamp() == output_entry->timestamp(), "serialize/deserialize test fail");
    ASSERT(input_entry.x() == output_entry->x(), "serialize/deserialize test fail");
    ASSERT(input_entry.y() == output_entry->y(), "serialize/deserialize test fail");
    ASSERT(input_entry.v_x() == output_entry->v_x(), "serialize/deserialize test fail");
    ASSERT(input_entry.v_y() == output_entry->v_y(), "serialize/deserialize test fail");
    ASSERT(input_entry.v_r() == output_entry->v_r(), "serialize/deserialize test fail");
    ASSERT(input_entry.direction() == output_entry->direction(), "serialize/deserialize test fail");
    
    return 0;
}