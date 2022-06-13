#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

struct timeval tv;

void sig_handler(int signo) {
    /* SLURM SIGCONT-->SIGTERM */

    if (signo == SIGINT) {
        printf("received SIGINT %d\n",(int)time(NULL));
    }
    if (signo == SIGCONT) {
        printf("received SIGCONT %d\n",(int)time(NULL));
    }
    if (signo == SIGALRM) {
        printf("received SIGALRM %d\n",(int)time(NULL));
    }
    if (signo == SIGUSR1) {
        printf("received SIGUSR1 %d\n",(int)time(NULL));
    }
    if (signo == SIGUSR2) {
        printf("received SIGUSR1 %d\n",(int)time(NULL));
    }
    if (signo == SIGTERM) {
        printf("received SIGTERM %d\n",(int)time(NULL)); // exit(0);
    }
}


int main(void) {
    char * ev;
    int task_size, task_rank;

    if (signal(SIGINT,  sig_handler) == SIG_ERR) printf("can't catch SIGINT\n");
    if (signal(SIGALRM, sig_handler) == SIG_ERR) printf("can't catch SIGALRM\n");
    if (signal(SIGCONT, sig_handler) == SIG_ERR) printf("can't catch SIGCONT\n");
    if (signal(SIGUSR1, sig_handler) == SIG_ERR) printf("can't catch SIGUSR1\n");
    if (signal(SIGUSR2, sig_handler) == SIG_ERR) printf("can't catch SIGUSR2\n");
    if (signal(SIGTERM, sig_handler) == SIG_ERR) printf("can't catch SIGTERM\n");

    ev = getenv("SLURM_STEP_NUM_TASKS");
    task_size =  (ev ? atoi( ev ) : 1);
    ev = getenv("SLURM_LOCALID");
    task_rank =  (ev ? atoi( ev ) : 0);

    // A long long wait so that we can easily issue a signal to this process
    while(1) {

        gettimeofday(& tv,NULL);

        if(task_rank==0)
            printf(
                "WALL %s %d %d %d\n",
                getenv("SLURM_JOB_ID"),
                task_size,
                task_rank,
                (int)tv.tv_sec
            );

        fflush(stdout);
        fflush(stdin);

        sleep(1);
    }

    return 0;
}
