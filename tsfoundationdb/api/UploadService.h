#ifndef UPLOAD_SERVICE_H_
#define UPLOAD_SERVICE_H_

#include <functional>
#include "DataService.h"
#include "../cache/CacheManager.h"
#include "../log/LogManager.h"

class UploadService {
    CacheManager cache_manager_;
    LogManager log_manager_;
    int tag_count_;

public:
    UploadService(int tag_count = 4, int length_upper_bound = 120);
    
    bool UploadData(MessageEntry *entry);

};

#endif