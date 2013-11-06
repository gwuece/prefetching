#include "cache-sim.h"
#include <stdlib.h>

void list_init(struct list *l)
{
	l->head = NULL;
	l->tail = NULL;
	l->cnt = 0;
}


void list_insert_head(struct list *l, struct list_node *n)
{
	n->prev = NULL;
	n->next = l->head;

	if (l->head)
		l->head->prev = n;

	l->head = n;

	if (l->tail == NULL)
		l->tail = n;

	l->cnt++;
}

void list_remove(struct list *l, struct list_node *n)
{
	if (n->prev) {
		n->prev->next = n->next;
	}
	if (n->next) {
		n->next->prev = n->prev;
	}

	if (l->head == n)
		l->head = n->next;

	if (l->tail == n)
		l->tail = n->prev;

	n->next = NULL;
	n->prev = NULL;
	l->cnt--;
}

struct list_node *list_remove_tail(struct list *l)
{
	struct list_node *n = l->tail;

	if (n) {
		l->tail = n->prev;
		l->cnt--;
		if (n->prev) {
			n->prev->next = NULL;
		}
		n->prev = NULL;
	}

	return n;
}


struct cache_state *cache_init(unsigned long long n_blocks, unsigned long long cache_size)
{
	int i;
	struct cache_state *c = malloc(sizeof(struct cache_state));

	if (!c) {
		cache_free(c);
		return NULL;
	}

	list_init(&c->lru);

	c->lut = malloc(n_blocks * sizeof(struct list_node));

	if (!c->lut) {
		cache_free(c);
		return NULL;
	}

	for (i=0; i<n_blocks; i++) {
		c->lut[i].id = i;
		c->lut[i].is_cached = 0;
		c->lut[i].is_prefetch = 0;
		c->lut[i].access_cnt = 0;
		c->lut[i].next = NULL;
		c->lut[i].prev = NULL;
	}

	c->hit = 0;
	c->miss = 0;
	c->error_cnt = 0;
	c->n_blocks = n_blocks;
	c->cache_size = cache_size;

        c->true_pos = 0; /* prefetched and used */
        c->false_pos = 0; /* prefetched and not used */
        c->false_neg = 0; /* not prefetched and used */

	return c;
}


void cache_clear(struct cache_state *c)
{
	struct list_node *r;

	while ((r = list_remove_tail(&c->lru)))
	{
		if (r->is_prefetch) {

		  if (r->access_cnt > 0)
		    c->true_pos++;
		  else
		    c->false_pos++;

		}
		else {
		  if (r->access_cnt > 0)
		    c->false_neg++;
		}

		r->is_cached = 0;
		r->is_prefetch = 0;
		r->access_cnt = 0;
	}
}

void cache_free(struct cache_state *c)
{
	if (c) {
		free(c->lut);
	}
}


int cache_access(struct cache_state *c, unsigned long long block_id, int prefetch_access)
{
	struct list_node *n;
	int is_hit = 0;

	if (block_id < 0 || block_id > c->n_blocks) {
		c->error_cnt++;
		return -1;
	}

	n = &c->lut[block_id];

	if (n->is_cached) {
		c->hit++;
		//printf("hit %lld\n", block_id);
		is_hit = 1;
	}
	else {
		c->miss++;
		//printf("miss %lld\n", block_id);
	}

	if (n->is_cached) {
		list_remove(&c->lru, n);
	}

	list_insert_head(&c->lru, n);

	n->is_cached = 1;
	n->is_prefetch = prefetch_access;
	n->access_cnt += prefetch_access;

	if (c->lru.cnt > c->cache_size) {
		struct list_node *r = list_remove_tail(&c->lru);

		if (r->is_prefetch) {

		  if (r->access_cnt > 0)
		    c->true_pos++;
		  else
		    c->false_pos++;

		}
		else {
		  if (r->access_cnt > 0)
		    c->false_neg++;
		}

		r->is_cached = 0;
		r->is_prefetch = 0;
		r->access_cnt = 0;

		//printf("Removed %lld\n", r->id);
		//printf("head = %lld\n", c->lru.head->id);
		//printf("tail = %lld\n", c->lru.tail->id);
	}

	return is_hit;
}
