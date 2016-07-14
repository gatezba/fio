//
// Created by 김태완 on 2016. 6. 15..
//

#ifndef RFIO_RC_H
#define RFIO_RC_H

#include <eredis.h>
#include "stat.h"

void redis_init();
void redis_connect(char* hostname);
void redis_send_status(struct jobs_eta *je);

#endif //RFIO_RC_H
