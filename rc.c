#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#include "rc.h"

eredis_t * hEredis = NULL;
eredis_reader_t *reader;


void redis_init()
{
    hEredis = eredis_new();
    eredis_timeout(hEredis, 200 );
    eredis_r_retry(hEredis, 1 );
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

    reader = eredis_r(hEredis);

    printf("Connect Redis(%s:%d).", target, port);
}

void redis_send_status(struct jobs_eta *je)
{
    int command;
    char buff[40];
    char timestr[20];
    eredis_reply_t *reply;
    time_t now = time(NULL);

    if (hEredis == NULL)
    {
        return;
    }

    strftime(timestr, 20, "%Y%m%d%H%M%S", localtime(&now));
    sprintf(buff, "nva:io:%s", timestr);

    eredis_w_cmd(hEredis, "HINCRBY %s thr %d", buff, je->rate[DDIR_READ]);
    eredis_w_cmd(hEredis, "HINCRBY %s thw %d", buff, je->rate[DDIR_WRITE]);
    eredis_w_cmd(hEredis, "HINCRBY %s tht %d", buff, je->rate[DDIR_READ] + je->rate[DDIR_WRITE]);
    eredis_w_cmd(hEredis, "HINCRBY %s ior %d", buff, je->iops[DDIR_READ]);
    eredis_w_cmd(hEredis, "HINCRBY %s iow %d", buff, je->iops[DDIR_WRITE]);
    eredis_w_cmd(hEredis, "HINCRBY %s iot %d", buff, je->iops[DDIR_READ] + je->iops[DDIR_WRITE]);
    eredis_w_cmd(hEredis, "HINCRBY %s cnt 1", buff);
}
