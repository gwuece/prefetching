#define _FILE_OFFSET_BITS 64
#include <stdio.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#define __USE_GNU /* For readahead() */
#define _XOPEN_SOURCE 600 /* posix_fadvise */
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <math.h>
#include <assert.h>
#include <signal.h>
#include <string.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <errno.h>
#include <sched.h>
#include "bitarray.h"
#include "blktrace_api.h"
#include "cache-sim.h"

double ratio_beta = 0.5;
double ratio_alpha = 0.9;
double accu_pct_old = 0;
double pol_pct_old = 0;
double len_old = 0;
double default_scale = 0;
int feedback_state = 0;//0: Init (program start. No previous history); 1: Normal (already has one previouos history), 2: Reset (Previous history discarded. Reset prefetcher to default size.)
//State diagram: 0->1->2 

char* timestamp(){//Created by Ron C. Chiang Sep. 6th. 2011
	time_t ltime; //calendar time
	ltime=time(NULL); //get current calendar time
	return asctime( localtime(&ltime));
}

double adjust_aggressivness(
	int true_pos,
	int false_neg,
	int false_pos,
	double old_scale,
	double app_tp, 
	double max_tp, 
	FILE *fp_trace
	)
{//Edited by Ron C. Chiang Sep. 8th. 2011
	double accu_pct = 0;
	double pol_pct = 0;
	double new_scale = 0;
	double new_len = 0;
	if(feedback_state==2){//Reset state. Prefetcher was reset.
//		printf("RESET to default SCALE = %.4f\n",default_scale); 
		feedback_state=0; //next state: program init
		accu_pct_old = accu_pct;
		pol_pct_old = pol_pct;
		//len_old = default_scale*app_tp;
		return default_scale;//because pattern recognizer enable prefetch. So, let's try it again
	}
	 
//	printf("%s\t - t_pos=%d\tf_pos=%d\tf_neg=%d\t",timestamp(),true_pos,false_pos,false_neg);
	if (true_pos + false_neg) {
		accu_pct = true_pos / (double) (true_pos + false_neg);
	}
//	printf("ACCU=%.4f\t",accu_pct);
        if(false_pos + true_pos) {
		pol_pct = false_pos / (double) (false_pos + true_pos);
        }
//	printf("POL=%.4f\t",pol_pct);

	if(accu_pct==0 || (old_scale*app_tp>=max_tp && accu_pct<1)){
		//which means true_pos == 0 or true_pos + false_neg == 0 
		//Or, already reach maximum size, but still some data is outside the prefetched data 
		//That is, inaccurate pattern recognition
		//Reset prefetcher no matter current state is init or normal
		new_scale = 0; pol_pct = 0;
		feedback_state=2;
//		printf("NEW_SCALE=%.4f\tInaccurate. Going to Reset\n",new_scale);
		return new_scale;
	}

	double L1=len_old;
	double L2=old_scale*app_tp;

	if((accu_pct + pol_pct) == 1){//perfect size. No need to change
		//keep current accu and pol
		new_scale = old_scale;
//		printf("NEW_SCALE=%.4f\tPerfect size. Keep it.\n",new_scale);	
	}else{
		if(feedback_state==0){//Init state
			if((accu_pct + pol_pct) > 1){//prefetch size is too big
				new_len = L2 / 2.0 ; //the spirit of binary search
				//Because origin is on small side. We cannot use it to calculate.
			}else if((accu_pct + pol_pct) <1){//prefetch size is too small
				new_len = (L2)/(accu_pct);	
			}
		}else if(feedback_state==1){//normal state
			//need to check three conditions
			if((accu_pct_old+pol_pct_old)<=1 && (accu_pct+pol_pct)<=1){
				//1. both on the small side
				new_len = L1 + (1-accu_pct_old)*(L2-L1)/(accu_pct-accu_pct_old);
			}else if((accu_pct_old+pol_pct_old)>=1 && (accu_pct+pol_pct)>=1){
				//2. both on the big side
				new_len = L2 - (L2-L1)*(pol_pct)/(pol_pct-pol_pct_old);
			}else{
				//3. one on the small side and one on the big side
				new_len = (L1 + L2)/2.0; //binary search 
			}
		}
		new_scale = new_len / app_tp;
//		printf("NEW_SCALE=%.4f\n",new_scale);
	}
	if(new_scale<=0){
		feedback_state=2;
		new_scale=0;
//		printf("Less than zero. Reset\n");
	}else{
		len_old = L2;
		feedback_state=1;
		//keep current accu and pol
		accu_pct_old = accu_pct;
		pol_pct_old = pol_pct;
	}
	return  new_scale;
}//end of adjust_aggressivness()

#define BUF_SIZE	(512 * 1024)
#define BUF_NR		(4)
#define MAX_CPUS        (16)

static int exit_flag;
static int act_mask = ~0U;
static unsigned long buf_size = BUF_SIZE;
static unsigned long buf_nr = BUF_NR;
static char buts_name[32];
static double red_block_threshold = 0.0;

static void sighandler(int sig)
{
	exit_flag = 1;
}

static int start_trace(int fd)
{
	struct blk_user_trace_setup buts;

	memset(&buts, 0, sizeof(buts));
	buts.buf_size = buf_size;
	buts.buf_nr = buf_nr;
	buts.act_mask = act_mask;

	if (ioctl(fd, BLKTRACESETUP, &buts) < 0) {
		perror("BLKTRACESETUP");
		return -1;
	}

	if (ioctl(fd, BLKTRACESTART) < 0) {
		perror("BLKTRACESTART");
		return -1;
	}

	memcpy(buts_name, buts.name, sizeof(buts_name));

	return 0;
}

static void stop_trace(int fd)
{
	if (fd <= 0)
		return;

	/*
	 * should be stopped, just don't complain if it isn't
	 */
	ioctl(fd, BLKTRACESTOP);

	if (ioctl(fd, BLKTRACETEARDOWN) < 0)
		perror("BLKTRACETEARDOWN");
}

#define MAXPATHLEN (2048)
static int get_dropped_count(const char *buts_name)
{
	int fd;
	char tmp[MAXPATHLEN + 64];
	static char default_debugfs_path[] = "/sys/kernel/debug";

	snprintf(tmp, sizeof(tmp), "%s/block/%s/dropped",
		 default_debugfs_path, buts_name);

	fd = open(tmp, O_RDONLY);
	if (fd < 0) {
		/*
		 * this may be ok, if the kernel doesn't support dropped counts
		 */
		if (errno == ENOENT)
			return 0;

		fprintf(stderr, "Couldn't open dropped file %s\n", tmp);
		return -1;
	}

	if (read(fd, tmp, sizeof(tmp)) < 0) {
		perror(tmp);
		close(fd);
		return -1;
	}

	close(fd);

	return atoi(tmp);
}

static int event_cmp(const void *a, const void *b)
{
	const struct blk_io_trace *e0 = *((const struct blk_io_trace **) a);
	const struct blk_io_trace *e1 = *((const struct blk_io_trace **) b);

	if (e0->time < e1->time) {
		return -1;
	}
	else if (e0->time > e1->time) {
		return 1;
	}

	return 0;
}

double gettime_double()
{
	struct timespec tp;
	if (clock_gettime(CLOCK_REALTIME, &tp) < 0) {
		perror("clock_gettime");
	}
	return tp.tv_sec + tp.tv_nsec * 1e-9;
}

int sleep_double(double t)
{
	struct timespec tp;
	tp.tv_sec = (time_t) t;
	tp.tv_nsec = (t - tp.tv_sec) * 1e9;
	return nanosleep(&tp, NULL);
}

struct timespec double_to_timespec(double t)
{
	assert(t >= 0.);

	struct timespec tp;
	tp.tv_sec = (time_t) t;
	tp.tv_nsec = (t - tp.tv_sec) * 1e9;
	return tp;
}

double timespec_to_double(struct timespec tp)
{
	return tp.tv_sec + tp.tv_nsec * 1e-9;
}


static char default_debugfs_path[] = "/sys/kernel/debug";
static int max_events = 65536;

struct blk_watch {
	int fd;
	int ncpus;
	struct pollfd trace_fd[MAX_CPUS];
	char *read_buf[MAX_CPUS];
	int used_bytes[MAX_CPUS];
	int processed_bytes[MAX_CPUS];
	struct blk_io_trace **event;
	unsigned int event_cnt;

	/* Replay mode */
	int replay;
	double t_prev;
	int peek_cnt;
	FILE *fp_trace;
	int trace_enable;
};

/* Forward declarations. */
int blkwatch_close(struct blk_watch *bw);

int blkwatch_init(struct blk_watch *bw, const char *path)
{
	int fd;
	int ncpus;
	FILE *fp_trace;

	struct stat st;
	int rc;

	memset(bw, 0, sizeof(*bw));

	rc = stat(path, &st);

	if (rc < 0) {
		perror("stat");
		return -1;
	}

	bw->event_cnt = 0;

	if (S_ISBLK(st.st_mode)) {
		/* Run trace on block device. */
		fd = open(path, O_RDONLY | O_NONBLOCK);

		if (fd < 0) {
			perror(path);
			blkwatch_close(bw);
			return -1;
		}

		ncpus = sysconf(_SC_NPROCESSORS_ONLN);

		if (ncpus < 0) {
			fprintf(stderr,
				"sysconf(_SC_NPROCESSORS_ONLN) failed\n");
			blkwatch_close(bw);
			return -1;
		}

		if (ncpus > MAX_CPUS) {
			fprintf(stderr,
				"ncpus %d > max %d\n", ncpus, MAX_CPUS);
			blkwatch_close(bw);
			return -1;
		}

		if (start_trace(fd) < 0) {
			blkwatch_close(bw);
			return -1;
		}

		int i;
		for (i=0; i<ncpus; i++) {
			char buf[80];
			snprintf(buf, sizeof(buf), "%s/block/%s/trace%d",
				 default_debugfs_path, buts_name, i);

			bw->trace_fd[i].fd = open(buf, O_RDONLY | O_NONBLOCK);

			if (bw->trace_fd[i].fd < 0) {
				perror(buf);
				break;
			}
			bw->trace_fd[i].events = POLLIN;
		}

		if (i != ncpus) {
			blkwatch_close(bw);
			return -1;
		}

		for (i=0; i<ncpus; i++) {
			bw->read_buf[i] =
				malloc(max_events * sizeof(struct blk_io_trace));

			if (bw->read_buf[i] == NULL) {
				blkwatch_close(bw);
				return -1;
			}
			bw->used_bytes[i] = 0;
		}

		bw->event = malloc(max_events * sizeof(struct blk_io_trace *));

		if (bw->event == NULL) {
			blkwatch_close(bw);
			return -1;
		}
	}
	else {
		/* Replay trace from ordinary file.
		 */
		fprintf(stderr,
			"Ordinary file replay not implemented.\n");
		blkwatch_close(bw);
		return -1;
	}


	fp_trace = fopen("prefetch.trace", "wb");

	if (!fp_trace) {
		blkwatch_close(bw);
		return -1;
	}

	bw->fd = fd;
	bw->ncpus = ncpus;

	bw->fp_trace = fp_trace;
	bw->trace_enable = 1;

	return 0;
}

int blkwatch_close(struct blk_watch *bw)
{
	int i;

	free(bw->event);

	for (i=0; i<bw->ncpus; i++) {
		if (bw->read_buf[i] == NULL)
			break;
		free(bw->read_buf[i]);
	}

	get_dropped_count(buts_name);

	for (i=0; i<bw->ncpus; i++) {
		if (bw->trace_fd[i].fd <= 0) {
			break;
		}
		close(bw->trace_fd[i].fd);
	}
	stop_trace(bw->fd);

	if (bw->fd > 0)
		close(bw->fd);

	if (bw->fp_trace)
		fclose(bw->fp_trace);

	return 0;
}


struct pred_linear {
	double sx;
	double sy;
	double sxy;
	double sx2;
	double sy2;
	int n;
	double x_min;
	double y_min;
	double x_max;
	double y_max;
	double slope;
	double intercept;
};

void pred_linear_init(struct pred_linear *p)
{
	p->sx = 0.;
	p->sy = 0.;
	p->sxy = 0.;
	p->sx2 = 0.;
	p->sy2 = 0.;
	p->n = 0;

	p->x_min = 10e37;
	p->x_max = -10e37;
	p->y_min = 10e37;
	p->y_max = -10e37;

	p->slope = 0.;
	p->intercept = 0.;
}

void pred_linear_point(struct pred_linear *p, double x, double y)
{
#if 0
	double m = p->slope;
	double b = p->intercept;

	double f = m * x + b;
	printf("%lf %lf %lf\n", x, y, f);
#endif

	p->sx += x;
	p->sy += y;
	p->sxy += x * y;
	p->sx2 += x * x;
	p->sy2 += y * y;
	p->n++;

	if (x < p->x_min) {
		p->x_min = x;
	}
	if (x > p->x_max) {
		p->x_max = x;
	}
	if (y < p->y_min) {
		p->y_min = y;
	}
	if (y > p->y_max) {
		p->y_max = y;
	}
}

double pred_linear_score(struct pred_linear *p)
{
	int n = p->n;
	double sx = p->sx;
	double sy = p->sy;
	double sx2 = p->sx2;
	double sy2 = p->sy2;
	double sxy = p->sxy;

	double cov = sxy / n - (sx / n) * (sy / n);
	double std_x = sqrt(sx2 / n - (sx / n) * (sx / n));
	double std_y = sqrt(sy2 / n - (sy / n) * (sy / n));


	if (n == 0 || n == 1) {
		p->slope = 0.;
		p->intercept = 0.;
		return 0.;
	}
#if 0
	printf("cov = %lf mx = %lf sx = %lf my = %lf sy = %lf\n",
	       cov,
	       sx / n,
	       std_x,
	       sy / n,
	       std_y);
#endif
	/* Least-squares regression. */

	double m = (sy * sx - n * sxy) / (sx * sx - n * sx2);
	double b = (sx * sxy - sy * sx2) / (sx * sx - n * sx2);

	p->slope = m;
	p->intercept = b;

	return cov / (std_x * std_y);
}


static int set_sched(int yes)
{
    if (yes) {
	struct sched_param sp;

	memset(&sp, 0, sizeof(sp));

	errno = 0;
	sp.sched_priority =
		sched_get_priority_max(SCHED_FIFO);

	if (sp.sched_priority < 0 && errno != 0) {
		perror("sched_get_priority");
		return -1;
	}

	if (sched_setscheduler(0 /* use our pid */,
			       SCHED_FIFO,
			       &sp) < 0)
	{
		perror("sched_setscheduler");
		return -1;
	}
    }

    return 0;
}

#define MAX_PREFETCH_HISTORY (512)

struct prefetch_operation {
	double t;
	off_t start_block;
	size_t n_blocks;
	unsigned char *used_array;
};

#define MIN(a,b) ((a) < (b) ? a : b)


size_t prefetch_operation_get_used_blocks(struct prefetch_operation *pp)
{
	size_t i, cnt = 0;

	for (i=0; i<pp->n_blocks; i++) {
		if (pp->used_array[i])
			cnt++;
	}

	return cnt;
}

void reduce_overlap(struct prefetch_operation *pp,
		    off_t start_block,
		    size_t n_blocks,
		    size_t *overlapping_blocks,
		    off_t *overlap_start,
		    off_t *remain_start,
		    off_t *remain_end)
{
	off_t a, b, c, d;
	off_t overlap_end;
	off_t i;

        a = start_block;
        b = start_block + n_blocks - 1;
        c = pp->start_block;
        d = pp->start_block + pp->n_blocks - 1;

        *overlap_start = 0;
        *overlapping_blocks = 0;
        *remain_start = 0;
        *remain_end = 0;

        if (b < c || d < a) {
		/* No overlap
		 */
		return;
	}
	else if (a >= c) {
		/* Partial overlap
		 * a====b
		 * c=====d
		 * c===d
		 */
		*overlap_start = a;
		overlap_end = MIN(b,d);
		*overlapping_blocks = overlap_end - *overlap_start + 1;

		if (b > d) {
			*remain_start = d + 1;
			*remain_end = b;
		}
	}
        else {
		/* Partial overlap
		 *   a====b
		 *    c====d
		 *    c==d
		 */
		*overlap_start = c;
		overlap_end = MIN(b,d);
		*overlapping_blocks = overlap_end - *overlap_start + 1;

		*remain_start = a;
		*remain_end = c - 1;
#if 0
		if (b > d) {
			exit(1);
		}
#endif
	}

        /* Mark overlapping */
	for (i=0; i<*overlapping_blocks; i++) {
		pp->used_array[*overlap_start - pp->start_block + i]++;
	}
}


typedef struct circ_buf_t {
	int head;
	int tail;
	unsigned int count;
	unsigned int len;
	unsigned int size;
	char *buf;
} circ_buf_t;

int circ_init(circ_buf_t *b, unsigned int len, unsigned int size)
{
	b->buf = malloc((len + 1) * size);

	if (!b->buf) {
		return -1;
	}

	b->len = (len + 1);
	b->size = size;
	b->head = 0;
	b->tail = 0;
	b->count = 0;

	return 0;
}

int circ_enq(circ_buf_t *b, const void *elm)
{
	int head = (b->head + 1) % b->len;

	if (head == b->tail) {
		return -1;
	}

	memcpy(b->buf + b->head * b->size, elm, b->size);
	b->head = head;
	b->count++;
	return 0;
}

int circ_deq(circ_buf_t *b, void *elm)
{
	if (b->head == b->tail) {
		return -1;
	}

	if (elm) {
		memcpy(elm, &b->buf[b->tail * b->size], b->size);
	}

	b->tail = (b->tail + 1) % b->len;
	b->count--;
	return 0;
}

void *circ_peek(circ_buf_t *b, int index)
{
	if (index >= b->count)
		return NULL;

	int i = (b->tail + index) % b->len;
	return &b->buf[i * b->size];
}

unsigned int circ_cnt(circ_buf_t *b)
{
	return b->count;
}

void circ_free(circ_buf_t *b)
{
	if (b) {
		free(b->buf);
	}
}

struct rg_region {
	size_t bytes_per_region;
	off_t max_id;
	unsigned int *predicted_and_read;
	unsigned int *predicted;
};

int rg_region_init(struct rg_region *rg, size_t bytes_per_region, off_t max_bytes)
{
	rg->bytes_per_region = bytes_per_region;
	rg->max_id =  max_bytes / bytes_per_region;
	rg->predicted_and_read = calloc(rg->max_id, sizeof(rg->predicted_and_read[0]));
	rg->predicted = calloc(rg->max_id, sizeof(rg->predicted[0]));
	return 0;
}

void rg_region_predicted(struct rg_region *rg, int read, off_t byte_offset, size_t n_bytes)
{
	off_t id = byte_offset / rg->bytes_per_region;

	if (id >= rg->max_id) {
		fprintf(stderr, "rg_region_predicted: bad offset %llu\n", byte_offset);
		return;
	}

	if (read) {
		rg->predicted_and_read[id] += n_bytes;
	}
	else {
		rg->predicted[id] += n_bytes;
	}
}

double rg_pct(struct rg_region *rg, off_t byte_offset)
{
	off_t id = byte_offset / rg->bytes_per_region;
	double pct = 1.0;


	if (id >= rg->max_id) {
		fprintf(stderr, "rg_region_pct: bad offset %llu\n", byte_offset);
		return 0.;
	}

	if (rg->predicted_and_read[id] + rg->predicted[id]) {
		pct = rg->predicted_and_read[id] / (double) (rg->predicted_and_read[id] + rg->predicted[id]);
	}

	return pct;
}

int main(int argc, char *argv[])
{
	set_sched(1);

	double interval = 0.025;
	double scale = 1.0;
	double consec_tol = 0.60;
	double history_time = 1.0;
	int prefetch_adaptive = 0;

	/* Max available throughput for every prefetcher.
	 */
	double max_throughput = 100e6;

	/* Use readahead or posix_fadvise to prefetch. The readahead
	 * call blocks until complete and the time loop tracks and
	 * accounts for this, so its performace is slightly better.
	 */
	int use_readahead = 1;


	if (getenv("MAX_THROUGHPUT")) {
		max_throughput = strtod(getenv("MAX_THROUGHPUT"), NULL);
		fprintf(stderr,
			"Set max_throughput = %lf\n",
			max_throughput);
	}


	if (getenv("PREFETCH_ADAPTIVE")) {
		prefetch_adaptive = strtol(
			getenv("PREFETCH_ADAPTIVE"), NULL, 0);
		fprintf(stderr,
			"Set prefetch_adaptive = %d\n",
			prefetch_adaptive);
	}

	if (getenv("RATIO_BETA")) {
		ratio_beta = strtod(getenv("RATIO_BETA"), NULL);
		fprintf(stderr,
			"Set ratio_beta = %lf\n",
			ratio_beta);
	}

	if (getenv("RATIO_ALPHA")) {
		ratio_alpha = strtod(getenv("RATIO_ALPHA"), NULL);
		fprintf(stderr,
			"Set ratio_alpha = %lf\n",
			ratio_alpha);
	}

	if (getenv("RED_BLOCK_THRESHOLD")) {
		red_block_threshold = strtod(getenv("RED_BLOCK_THRESHOLD"), NULL);
		fprintf(stderr,
			"Set red_block_threshold = %lf\n",
			red_block_threshold);
	}

	if (getenv("SCALE")) {
		default_scale = scale = strtod(getenv("SCALE"), NULL);
		fprintf(stderr,
			"Set scale = %lf\n",
			scale);
	}

	if (getenv("INTERVAL")) {
		interval = strtod(getenv("INTERVAL"), NULL);
		fprintf(stderr,
			"Set interval = %lf\n",
			interval);
	}

	if (getenv("CONSEC_TOL")) {
		consec_tol = strtod(getenv("CONSEC_TOL"), NULL);
		fprintf(stderr,
			"Set consec_tol = %lf\n",
			consec_tol);
	}

	int disable_prefetch = 0;

	/* Path to device for readahead.
	 */
	char *readahead_path = "/mnt/sdc1/tmp/span";

	/* Path to run block trace on:
	 */
	char *trace_path = "/dev/loop0";
	struct stat st;
	int fd;
	int rc;

	if (argc > 1) {
		trace_path = argv[1];
		printf("trace_path=%s\n",trace_path);
	}

	if (argc > 2) {
		readahead_path = argv[2];
	}

	if (argc > 3) {
		disable_prefetch = strtol(argv[3], NULL, 0);
		if (disable_prefetch)
			fprintf(stderr, "Warning: prefetching disabled\n");
	}

	fd = open(readahead_path, O_RDONLY | O_NONBLOCK);

	if (fd < 0) {
		perror("open");
		goto bad0;
	}

	rc = fstat(fd, &st);

	if (rc < 0) {
		perror("stat");
		goto bad1;
	}

	/* off_t is signed on this system, so comparisons with 0 are
	 * meaningful.
	 */

	off_t blk_size = 512;
	off_t max_block = st.st_size / blk_size;

	struct rg_region red_green;
	rg_region_init(&red_green, 1048576, st.st_size);
	struct blk_watch bw;

	struct timespec overall_start_time_tp;
	if (clock_gettime(CLOCK_REALTIME, &overall_start_time_tp) < 0) {
		perror("clock_gettime");
	}

	unsigned long long overall_start_timestamp =
		overall_start_time_tp.tv_sec * 1000000000ull
		+ overall_start_time_tp.tv_nsec;
	double overall_start_time = timespec_to_double(overall_start_time_tp);

	if (blkwatch_init(&bw, trace_path)) {
		perror("blkwatch_init");
		return 1;
	}

	signal(SIGINT, sighandler);
	signal(SIGHUP, sighandler);
	signal(SIGTERM, sighandler);
	signal(SIGALRM, sighandler);
	signal(SIGSEGV, sighandler);
	signal(SIGBUS, sighandler);

	int pref_event_cnt = 0, pref_blk_cnt = 0;

	double think_start_time, think_end_time;

	double sleep_time = interval;

	double elapsed_time = 0.;
	int interval_expired = 0;

	pid_t our_pid = getpid();

	fprintf(bw.fp_trace, "pid = %d\n", our_pid);

	fprintf(bw.fp_trace, "overall_start_time = %llu\n",
		overall_start_timestamp);

	fflush(bw.fp_trace);

	unsigned long long pref_read_bytes = 0;

	size_t tot_blk_cnt = 0;
	unsigned long long tot_recent_hit = 0;
	unsigned long long tot_recent_miss = 0;

	unsigned long long tot_cache_hit = 0;
	unsigned long long tot_cache_miss = 0;
	unsigned long long tot_false_pos = 0;

	unsigned long long red_counter = 0;

	unsigned long long tot_prefetch_and_unused = 0;
	unsigned long long tot_prefetch_and_used = 0;
	unsigned long long tot_unprefetch_and_used = 0;

	int tot_prefetch_enabled = 0;

#define HT_LEN (83)

	struct prefetcher_state {
		pid_t pid;
		int event_cnt;
		int blk_cnt;
		off_t min_seen_block;
		off_t max_seen_block;

		off_t curr_block_lo;
		off_t curr_block_hi;
		off_t prev_block_lo;
		off_t prev_block_hi;

		int consec_blk_cnt;
		int reverse_blk_cnt;
		struct pred_linear pl;
		double app_throughput;

		int prefetch_enable;
		int blk_dir;
		off_t start_block;
		off_t stop_block;
		off_t blocks_on;
		off_t blocks_off;

		int curr_gap_dir;
		int curr_gap_req_len;
		off_t curr_gap;
		off_t prev_gap;
		int strided_blk_cnt;

		unsigned long long read_bytes;

		off_t prev_end_block;
		off_t curr_consec_block_hi;

		/* Log previous prefetch operations to compare agains
		 * actual results.
		 */

		struct circ_buf_t prefetch_history;
		int recent_miss;
		int recent_hit;

		int cache_miss;
		int cache_hit;
		int false_pos;

		unsigned int recent_prefetch_and_unused;
		unsigned int recent_prefetch_and_used;
		unsigned int recent_unprefetch_and_used;

	} pf_table[HT_LEN];

	int i;
	for (i=0; i<HT_LEN; i++) {
		struct prefetcher_state *pf = &pf_table[i];

		pf->pid = 0;
		pf->event_cnt = 0;
		pf->blk_cnt = 0;
		pf->min_seen_block = max_block;
		pf->max_seen_block = 0;
		pf->curr_block_lo = 0;
		pf->curr_block_hi = 0;
		pf->prev_block_lo = 0;
		pf->prev_block_hi = 0;
		pf->consec_blk_cnt = 0;;
		pf->reverse_blk_cnt = 0;
		pf->app_throughput = 0.;
		pf->prefetch_enable = 0;
		pf->prev_gap = 0;
		pf->strided_blk_cnt = 0;
		pf->read_bytes = 0;
		pred_linear_init(&pf->pl);
		circ_init(&pf->prefetch_history,
			  MAX_PREFETCH_HISTORY,
			  sizeof(struct prefetch_operation));

		pf->recent_miss = 0;
		pf->recent_hit = 0;

		pf->recent_prefetch_and_used = 0;
		pf->recent_prefetch_and_unused = 0;
		pf->recent_unprefetch_and_used = 0;

		pf->cache_miss = 0;
		pf->cache_hit = 0;
		pf->false_pos = 0;

		pf->curr_gap = 0;
	}

	fprintf(bw.fp_trace, "getpagesize = %d reserving %d\n",
		getpagesize(),
		(int) (600e3/getpagesize())
		);

	off_t max_offset = 120000000000ull;
	double cache_mem = 600e6;
	int pagesize = 4096;
	int blocks_per_page = pagesize / 512;

	struct cache_state *sim_cache = cache_init(
		max_offset / pagesize,
		cache_mem / pagesize);

	unsigned long long initial_timestamp = 0;
	interval_expired = 1;

	while (!exit_flag) {

		think_start_time = gettime_double();

		/* Pretend poll succeeded */
		for (i=0; i<bw.ncpus; i++) {
			bw.trace_fd[i].revents = POLLIN;
		}

		for (i=0; i<bw.ncpus; i++) {
			int unused_bytes
				= max_events * sizeof(struct blk_io_trace);

			if (bw.trace_fd[i].revents & POLLIN || exit_flag) {
				char *dst = bw.read_buf[i] + bw.used_bytes[i];

				ssize_t rc = read(bw.trace_fd[i].fd,
						  dst,
						  unused_bytes);

				if (rc < 0 && errno != EAGAIN) {
					perror("read");
					exit_flag = 1;
				}

				/* Reads from this device always
				 * seem to return 0, so this may not
				 * be needed.
				 */

				if (rc < 0 && errno == EAGAIN) {
					rc = 0;
				}

				bw.used_bytes[i] += rc;
				unused_bytes -= rc;
				dst += rc;

				if (unused_bytes == 0) {
					fprintf(stderr,
						"Event buffer overflow\n");
				}
			}
		}


		double read_end_time = gettime_double();

		fprintf(bw.fp_trace,
			"trace read time is %lf\n",
			read_end_time - think_start_time);
		fflush(bw.fp_trace);

		/* Find events in each buffer */
		bw.event_cnt = 0;
		memset(bw.processed_bytes, 0, sizeof(bw.processed_bytes));

		for (i=0; i<bw.ncpus; i++) {
			int used = bw.used_bytes[i];
			char *blk_c = bw.read_buf[i];

			while (blk_c < &bw.read_buf[i][used])
			{

			struct blk_io_trace *blk = (struct blk_io_trace *) blk_c;

			blk_c += sizeof(struct blk_io_trace);

			if (blk_c > &bw.read_buf[i][used])
				break;

			bw.processed_bytes[i] += sizeof(struct blk_io_trace);

			__u32 magic = blk->magic;

			if ((magic & 0xffffff00) != BLK_IO_TRACE_MAGIC) {
				fprintf(stderr, "Bad magic %x\n", magic);
			}

			blk_c += blk->pdu_len;

			if (blk_c > &bw.read_buf[i][used])
				break;

			bw.processed_bytes[i] += blk->pdu_len;
#if 0
			fprintf(stderr,
			"seq %u time %llu pid %u sector %llu len %u act 0x%x\n",
				blk->sequence,
				blk->time,
				blk->pid,
				blk->sector,
				blk->bytes,
				blk->action
			);
#endif

			if (initial_timestamp == 0) {
				initial_timestamp = blk->time;

				fprintf(bw.fp_trace, "Setting initial_timestamp = %llu\n", initial_timestamp);
			}

			/* Convert length to block count. */
			blk->bytes /= blk_size;

			/* Filter as needed. */

			if ((blk->action & 0xffff) != __BLK_TA_QUEUE)
				continue;

			if ((blk->action & BLK_TC_ACT(BLK_TC_READ)) == 0)
				continue;

			/* Ignore pid 0 -- kernel stuff */
			if (blk->pid == 0)
				continue;

			if (blk->pid == our_pid) {
#if 0
				pref_event_cnt++;
				pref_blk_cnt += blk->bytes;
#endif
				continue;
			}

			bw.event[bw.event_cnt] = blk;
			bw.event_cnt++;
			}
		}

		/* Sort by timestamp. */
		qsort(bw.event,
		      bw.event_cnt,
		      sizeof(struct blk_io_trace *),
		      event_cmp);

		if (initial_timestamp == 0 && bw.event_cnt > 0) {
			initial_timestamp = bw.event[0]->time;
		}

		for (i=0; i<bw.event_cnt; i++) {
			struct blk_io_trace *ba = bw.event[i];

			ba->time -= initial_timestamp;

			if (bw.trace_enable) {
#if 1
				fprintf(bw.fp_trace,
					"Actu, %lf, %llu,   , %d, %u, seq=%u\n",
					ba->time * 1e-9,
					ba->sector,
					ba->bytes,
					ba->pid,
					ba->sequence
					);
				fflush(bw.fp_trace);
#endif
			}

			pid_t hash = (ba->pid + (ba->sector / 4000000) ) * 16851 % HT_LEN;
			struct prefetcher_state *pf = &pf_table[hash];

			pf->pid = ba->pid;

			/* Weight a multiple block request N times. */
			int i;
#if 1
			for (i=0; i<ba->bytes / 8; i++) {
				pred_linear_point(&pf->pl,
						  ba->time * 1e-9,
						  ba->sector);
			}
#endif
			pf->event_cnt++;
			pf->blk_cnt += ba->bytes;
			tot_blk_cnt += ba->bytes;

			/* Compute cache hit or miss
			 */
			off_t page_start = ba->sector / blocks_per_page;
			int page_len = ceil(ba->bytes / blocks_per_page);

#if 0
			for (i=0; i<page_len; i++) {
			  int is_hit = cache_access(sim_cache, page_start + i, 0 /* is_pref */);
			  if (is_hit) {
			    pf->cache_hit++;
			  }
			  else {
			    pf->cache_miss++;
			  }
			}
#endif

			/* Find in history.
			 */
			struct prefetch_operation *pp;

			off_t start_block = ba->sector;
			off_t n_blocks = ba->bytes;

#if 1
			for (i=0; i<circ_cnt(&pf->prefetch_history); i++)
			{
				pp = circ_peek(
					&pf->prefetch_history,
					i);

				if (!pp) {
					break;
				}

				if (pp->t + history_time >= elapsed_time)
				{
					off_t overlap_start;
					size_t overlapping_blocks;
					off_t remain_start;
					off_t remain_end;

					reduce_overlap(pp,
						       start_block,
						       n_blocks,
						       &overlapping_blocks,
						       &overlap_start,
						       &remain_start,
						       &remain_end);

					if (overlapping_blocks > 0) {
						start_block = remain_start;
						n_blocks = remain_end - remain_start + 1;
						

					}
				}
			}
#endif

			pf->recent_unprefetch_and_used += n_blocks;

			pf->recent_miss += n_blocks;
			pf->recent_hit += ba->bytes - n_blocks;

			if (ba->sector < pf->min_seen_block) {
				pf->min_seen_block = ba->sector;
			}

			if (ba->sector > pf->max_seen_block) {
				pf->max_seen_block = ba->sector;
			}

			pf->prev_gap = pf->curr_gap;
			pf->prev_block_lo = pf->curr_block_lo;
			pf->prev_block_hi = pf->curr_block_hi;

			pf->curr_block_lo = ba->sector;
			pf->curr_block_hi = ba->sector + ba->bytes;

			if (pf->prev_block_hi == pf->curr_block_lo) {
				pf->consec_blk_cnt += ba->bytes;
				pf->curr_consec_block_hi = pf->curr_block_hi;
			}

			if (pf->prev_block_lo == pf->curr_block_hi) {
				pf->reverse_blk_cnt += ba->bytes;
			}

			if (pf->prev_block_hi <= pf->curr_block_lo) {
				pf->curr_gap = pf->curr_block_lo - pf->prev_block_hi;
				pf->curr_gap_dir = 1;
				pf->curr_gap_req_len = ba->bytes;
			}
			if (pf->prev_block_lo >= pf->curr_block_hi) {
				pf->curr_gap = pf->curr_block_hi - pf->prev_block_lo;
				pf->curr_gap_dir = -1;
				pf->curr_gap_req_len = ba->bytes;
			}

			if (pf->curr_gap == pf->prev_gap) {
				/* Should we check the prev req len? */
				pf->strided_blk_cnt += ba->bytes;
			}
		}


		sleep_time = interval;
		elapsed_time = think_start_time - overall_start_time;

		for (i=0; i<HT_LEN; i++)
		{
			struct prefetcher_state *pf = &pf_table[i];

			if (pf->event_cnt == 0) {
				continue;
			}

			double r = pred_linear_score(&pf->pl);
			double x_min = 0., x_max = 0;

			if (pf->event_cnt > 0) {
				x_min = pf->pl.x_min;
				x_max = pf->pl.x_max;
			}

			pf->app_throughput = pf->blk_cnt * blk_size / (x_max - x_min);

			double measured_prefetch_throughput
				= pref_blk_cnt * blk_size / interval;

			double consec_pct = 0.;
			double reverse_pct = 0.;
			double strided_pct = 0.;

			if (pf->event_cnt > 0)
				consec_pct = (double) pf->consec_blk_cnt / pf->blk_cnt;

			if (pf->event_cnt > 0)
				reverse_pct = (double) pf->reverse_blk_cnt / pf->blk_cnt;

			if (pf->event_cnt > 0)
				strided_pct = (double) pf->strided_blk_cnt / pf->blk_cnt;


			int attempt_enable_prefetch = 0;

			if (consec_pct > consec_tol ) {

				double prefetch_throughput = scale * pf->app_throughput;

				if (prefetch_throughput > max_throughput) {
					prefetch_throughput = max_throughput;
				}

				fprintf(bw.fp_trace,
					"prefetch_throughput = %lf app_throughput = %lf\n", prefetch_throughput, pf->app_throughput);


				pf->start_block = pf->curr_block_hi;

				pf->stop_block = pf->curr_block_hi
						 + (interval * prefetch_throughput / blk_size);



				if (pf->start_block < pf->prev_end_block
				    && (pf->prev_end_block - pf->start_block) * blk_size / pf->app_throughput < 4 * interval)
				{
					pf->start_block = pf->prev_end_block + 1;

					if (pf->stop_block < pf->start_block)
						pf->stop_block = pf->start_block
								 + (interval * prefetch_throughput / blk_size);

				}

				attempt_enable_prefetch = 1;
			}



			if (attempt_enable_prefetch) {

				if (pf->start_block < 0) {
					pf->start_block = 0;
				}

				if (pf->stop_block < 0) {
					pf->stop_block = 0;
				}

				if (pf->start_block > max_block) {
					pf->start_block = max_block;
				}

				if (pf->stop_block > max_block) {
					pf->stop_block = max_block;
				}

				if (pf->start_block <= pf->stop_block) {
					pf->prefetch_enable = 1;
					tot_prefetch_enabled++;
				}
			}

#if 1
			double lag = elapsed_time - x_min;

			fprintf(bw.fp_trace,
				"Elp %2.2lf [%d] %d n %3d %3d %+.3lf R %3.0lf %3.0lf %.3lf to %.3lf\n cp %2.0lf rp %3.0lf sp %3.0lf %lld %lld gap %d %lld lag %lf\n",
				elapsed_time,
				pf->pid,
				pf->prefetch_enable,
				pf->event_cnt,
				pref_event_cnt,
				r,
				100. * pf->app_throughput / max_throughput,
				100. * measured_prefetch_throughput / max_throughput,
				x_min,
				x_max,
				100. * consec_pct,
				100. * reverse_pct,
				100. * strided_pct,
				pf->min_seen_block,
				pf->max_seen_block,
				pf->curr_gap_dir,
				pf->curr_gap,
				lag
			       );
			fflush(bw.fp_trace);

			double pct = 0.;

			if (pf->recent_hit + pf->recent_miss) {
				pct = pf->recent_hit
				    / (double) (pf->recent_hit + pf->recent_miss);
			}

			fprintf(bw.fp_trace,
				"Recent hit %d miss %d rate %lf\n",
				pf->recent_hit,
				pf->recent_miss,
				pct
				);
			fflush(bw.fp_trace);

			pct = 0.;

			if (pf->cache_hit + pf->cache_miss) {
				pct = pf->cache_hit
				    / (double) (pf->cache_hit + pf->cache_miss);
			}

			fprintf(bw.fp_trace,
				"Cache hit %d miss %d rate %lf\n",
				pf->cache_hit,
				pf->cache_miss,
				pct
				);
			fflush(bw.fp_trace);
#endif

			if (pf->prefetch_enable) {

				off_t pref_blk_len =
					pf->stop_block - pf->start_block + 1;

				fprintf(bw.fp_trace,
					"Prefetch %llu to %llu blocks %llu total for pid %d thr %g\n",
					pf->start_block,
					pf->stop_block,
					pref_blk_len,
					pf->pid,
					(pf->stop_block - pf->start_block) / interval
					);
				fflush(bw.fp_trace);

				ssize_t rc;
				double read_start = gettime_double();
				double read_end = read_start;

				if (bw.trace_enable) {
					fprintf(bw.fp_trace,
						"Pref, %lf,   , %lld, %lld\n",
						elapsed_time,
						pf->start_block,
						pref_blk_len);

					fprintf(bw.fp_trace,
						"Hist %u\n",
						circ_cnt(&pf->prefetch_history));

					fflush(bw.fp_trace);

					/* Add prefetched blocks to the cache
					 */
					off_t page_start = pf->start_block / blocks_per_page;
					int page_len = ceil(pref_blk_len / blocks_per_page);

#if 0
					for (i=0; i<page_len; i++) {
					  cache_access(sim_cache, page_start + i, 1 /* is_pref */);
					}
#endif

					/* Remove stale history
					 * starting from the oldest.
					 */

					fprintf(bw.fp_trace,
						"Purge history\n");
					fflush(bw.fp_trace);
						
					struct prefetch_operation *pp;
					do {
						pp = circ_peek(
							&pf->prefetch_history,
							0);

						if (!pp || pp->t + history_time
						    >= elapsed_time)
						{
							break;
						}

						struct prefetch_operation pop;

						circ_deq(&pf->prefetch_history,
							 &pop);


						size_t used = prefetch_operation_get_used_blocks(&pop);

						size_t unused = pop.n_blocks - used;

						pf->recent_prefetch_and_used += used;
						pf->recent_prefetch_and_unused += unused;

						/* mark spatial predicition */
						int j;
						for (j=0; j<pop.n_blocks; j++) {
							int read = 0;
							if (pop.used_array[j]) {
								read = 1;
							}
							rg_region_predicted(&red_green,
									    read,
									    (pop.start_block + j) * blk_size,
									    blk_size);
						}

						free(pop.used_array);
					}while (pp);

					fprintf(bw.fp_trace,
						"Done purging history\n");

					fprintf(bw.fp_trace,
						"Recent true pos %d  false neg %d false pos %d\n",
						pf->recent_hit,
						pf->recent_miss,
						pf->false_pos);

					fflush(bw.fp_trace);

					if (prefetch_adaptive) {

						scale = 
						adjust_aggressivness(
							pf->recent_prefetch_and_used,
							pf->recent_prefetch_and_unused,
							pf->recent_unprefetch_and_used,
							scale,
							pf->app_throughput,
							max_throughput,
bw.fp_trace);

					fprintf(bw.fp_trace,
						"adj scale to %lf\n", scale);
					}


					struct prefetch_operation p;
					p.t = elapsed_time;
					p.start_block = pf->start_block;
					p.n_blocks = pref_blk_len;

					p.used_array = calloc(p.n_blocks, sizeof(unsigned char));

					/* end points to just after prefetch op ends */

					circ_enq(&pf->prefetch_history,
						 &p);
				}//if (bw.trace_enable)
#if 0
				int color = 1;

				if (rg_pct(&red_green, ((pf->start_block + pf->stop_block) / 2) * blk_size) >= red_block_threshold)
					color = 0;
#else
				int color = 0;

				if (rg_pct(&red_green, ((pf->start_block + pf->stop_block) / 2) * blk_size) < red_block_threshold)
					color = 1;
#endif

				fprintf(bw.fp_trace,
					"call readahead\n");
				fflush(bw.fp_trace);

				if (disable_prefetch || color){
					red_counter += pref_blk_len;
					rc = 0;
				}else {
					if (use_readahead) {
						rc = readahead(
							fd,
							pf->start_block * blk_size,
							pref_blk_len * blk_size);
					}else {//a --- typo a --> { unmatch brackets fixed by Ron C. Chiang Sep 8th 2011OD
						rc = posix_fadvise(
							fd,
							pf->start_block * blk_size,
							pref_blk_len * blk_size,
							POSIX_FADV_WILLNEED
							);
					}
				}


				if (rc) {
					perror("readahead");
				}

				pref_event_cnt++;
				pref_blk_cnt += pref_blk_len;

				read_end = gettime_double();

				size_t n_bytes_read = 0;
				n_bytes_read += blk_size * pref_blk_len;

				fprintf(bw.fp_trace,
					"Read time %lf %g Bps \n", read_end - read_start, n_bytes_read / (read_end - read_start));

				fflush(bw.fp_trace);
				pf->prev_end_block = pf->stop_block;
				pf->prefetch_enable = 0;
			}//if(pf->prefetch_enable)


			pf->read_bytes += pf->blk_cnt * blk_size;
			pref_read_bytes += pref_blk_cnt * blk_size;

			tot_recent_miss += pf->recent_miss;
			tot_recent_hit += pf->recent_hit;

			tot_cache_miss += pf->cache_miss;
			tot_cache_hit += pf->cache_hit;

			tot_false_pos += pf->false_pos;

			tot_prefetch_and_used += pf->recent_prefetch_and_used;
			tot_prefetch_and_unused += pf->recent_prefetch_and_unused;
			tot_unprefetch_and_used += pf->recent_unprefetch_and_used;


			pf->recent_prefetch_and_used = 0;
			pf->recent_prefetch_and_unused = 0;
			pf->recent_unprefetch_and_used = 0;

			pf->recent_miss = 0;
			pf->recent_hit = 0;

			pf->cache_miss = 0;
			pf->cache_hit = 0;
			pf->false_pos = 0;

			pf->event_cnt = 0;
			pf->blk_cnt = 0;
			pref_event_cnt = 0;
			pref_blk_cnt = 0;
			pred_linear_init(&pf->pl);
			pf->consec_blk_cnt = 0;
			pf->reverse_blk_cnt = 0;
			pf->strided_blk_cnt = 0;
			pf->min_seen_block = max_block;
			pf->max_seen_block = 0;
		}//end of for (i=0; i<HT_LEN; i++)

		think_end_time = gettime_double();
		tot_prefetch_enabled = 0;
		sleep_time -= think_end_time - think_start_time;

#if 1
		fprintf(bw.fp_trace,
			"Think took %lf now sleep %lf\n",
			think_end_time - think_start_time,
			sleep_time);
		fflush(bw.fp_trace);
#endif
		/* Reset counters */
		bw.event_cnt = 0;
		for (i=0; i<bw.ncpus; i++) {
			bw.used_bytes[i] = 0;
		}

		ssize_t poll_rc = -1;
		struct timespec timeout;

		if (sleep_time < 0.) {
			timeout.tv_sec = 0;
			timeout.tv_nsec = 0;
		}
		else {
			timeout = double_to_timespec(sleep_time);
		}

		if (!exit_flag 
		    && (poll_rc = ppoll(bw.trace_fd,
					bw.ncpus,
					&timeout,
					NULL /* sigmask */
				)) < 0
		    && errno != EINTR)
		{
			perror("poll");
			exit_flag = 1;
		}
	}//end of while (!exit_flag) 

	double pct = 0.;	

	if (tot_recent_hit + tot_recent_miss) {
		pct = tot_recent_hit
		      / (double) (tot_recent_hit + tot_recent_miss);
	}

	fprintf(bw.fp_trace,
		"Recent %llu hits %llu misses %lf pct (false pos %llu)\n",
		tot_recent_hit,
		tot_recent_miss,
		pct,
		tot_false_pos
		);


	if (tot_recent_hit + tot_recent_miss) {
		pct = tot_recent_hit
		      / (double) (tot_recent_hit + tot_recent_miss);
	}

	fprintf(bw.fp_trace,
		"Recent %llu pref_and_used %llu pref_and_unused %llu unpref_and_used\n",
		tot_prefetch_and_used,
		tot_prefetch_and_unused,
		tot_unprefetch_and_used
		);

	fprintf(bw.fp_trace,
		"prefetch_and_used / tot_used = %lf prefetch_and_used / prefetched = %lf\n",
		(double) tot_prefetch_and_used / (tot_blk_cnt),
		(double) tot_prefetch_and_used / (tot_prefetch_and_used + tot_prefetch_and_unused));

	cache_clear(sim_cache);

	pct = 0.;	

	if (tot_cache_hit + tot_cache_miss) {
		pct = tot_cache_hit
		      / (double) (tot_cache_hit + tot_cache_miss);
	}

	fprintf(bw.fp_trace,
		"Cache %llu hits %llu misses %lf pct\n",
		tot_cache_hit,
		tot_cache_miss,
		pct);


	fprintf(bw.fp_trace,
		"true pos (prefetched and used):      %llu\n"
		"false pos (prefetched and not used): %llu\n"
		"false neg (not prefetched and used): %llu\n",
		sim_cache->true_pos,
		sim_cache->false_pos,
		sim_cache->false_neg
		);

	fprintf(bw.fp_trace,
		"Read %llu bytes %3.0lf MBs\n",
		tot_blk_cnt * blk_size,
		1e-6 * tot_blk_cnt * blk_size / elapsed_time
	       );

	fprintf(bw.fp_trace,
		"Read %llu prefetch bytes %3.0lf MBs\n",
	       pref_read_bytes,
	       1e-6 * pref_read_bytes / elapsed_time
	       );

	fprintf(bw.fp_trace,
		"Red counter %llu\n", red_counter);

	fflush(bw.fp_trace);

	fprintf(stderr, "prefetchd closing trace device\n");

	fflush(bw.fp_trace);

	cache_free(sim_cache);

	blkwatch_close(&bw);
	close(fd);
	fprintf(stderr, "prefetchd exit\n");
	return 0;


bad1:
	close(fd);
bad0:
	return 1;

}
