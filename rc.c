#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#include "rc.h"

eredis_t * hEredis;

void redis_init()
{
    hEredis = eredis_new();
}

void redis_connect(char* target)
{
    int port = 6379;

    char* sep = strchr(target, ':');
    if (sep)
    {
        *sep = '\0';
        port = atoi(sep+1);
    }

    eredis_host_add(hEredis, target, port );
    eredis_run_thr(hEredis);

    printf("Connect Redis(%s:%d).", target, port);
}

void redis_send_status(struct jobs_eta *je)
{
    int command;
    char buff[40];
    char timestr[20];
    eredis_reader_t *reader;
    eredis_reply_t *reply;
    time_t now = time(NULL);

    strftime(timestr, 20, "%Y%m%d%H%M%S", localtime(&now));
    sprintf(buff, "nva:io:%s", timestr);

    reader = eredis_r(hEredis);
    reply = eredis_r_cmd(reader, "EXISTS %s", buff);
    if (!reply)
    {
        printf("failed to exists key %s", buff);
        return;
    }

    command = reply->integer;
    eredis_r_release(reader);

    switch (command)
    {
        case 0: {
            eredis_w_cmd(hEredis, "HMSET %s thr %d thw %d tht %d ior %d iow %d iot %d", buff,
                         je->rate[DDIR_READ], je->rate[DDIR_WRITE], je->rate[DDIR_READ] + je->rate[DDIR_WRITE],
                         je->iops[DDIR_READ], je->iops[DDIR_WRITE], je->iops[DDIR_READ] + je->iops[DDIR_WRITE]);
            break;
        }
        case 1: {
            eredis_w_cmd(hEredis, "HINCRBY %s thr %d", buff, je->rate[DDIR_READ]);
            eredis_w_cmd(hEredis, "HINCRBY %s thw %d", buff, je->rate[DDIR_WRITE]);
            eredis_w_cmd(hEredis, "HINCRBY %s tht %d", buff, je->rate[DDIR_READ] + je->rate[DDIR_WRITE]);
            eredis_w_cmd(hEredis, "HINCRBY %s ior %d", buff, je->iops[DDIR_READ]);
            eredis_w_cmd(hEredis, "HINCRBY %s iow %d", buff, je->iops[DDIR_WRITE]);
            eredis_w_cmd(hEredis, "HINCRBY %s iot %d", buff, je->iops[DDIR_READ] + je->iops[DDIR_WRITE]);
            break;
        }
    }
    eredis_w_cmd(hEredis, "expire %s 60", buff);
    eredis_w_cmd(hEredis, "SET nva:lasttime %s", timestr);
}
