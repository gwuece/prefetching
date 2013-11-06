#include "bitarray.h"
#include <stdlib.h>
#include <limits.h> /* PAGESIZE */

int bitarray_init(struct bitarray *b, unsigned int len)
{

	unsigned int n_bytes;
	unsigned char *p;

	n_bytes = (len + 31) / 32;
#ifndef BITARRAY_DEBUG
	p = calloc(n_bytes, 1);
#else
	n_bytes = n_bytes + 2 * PAGESIZE - 1;
	p = calloc(n_bytes, 1);

	/* Align to a multiple of PAGESIZE, assumed to be a power of two */
	p = (char *)(((unsigned long) p + PAGESIZE - 1) & ~(PAGESIZE - 1));
#endif

	if (!p) {
		b->p = NULL;
		b->len = 0;
		return -1;
	}


	b->p = p;
	b->len = len;
	return 0;
}


int bitarray_get(const struct bitarray *b, unsigned int i)
{
	unsigned int word = i / 32;
	unsigned int bit = i % 32;

#if BITARRAY_DEBUG
	assert(i < b->len);
#endif
	return (b->p[word] & (1 << bit)) >> bit;
}

void bitarray_set(struct bitarray *b, unsigned int i, unsigned int val)
{
	unsigned int word = i / 32;
	unsigned int bit = i % 32;

#if BITARRAY_DEBUG
	assert(i < b->len);
#endif
	if (val) {
		b->p[word] |= (1 << bit);
	}
	else {
		b->p[word] &= ~(1 << bit);
	}
}

void bitarray_free(struct bitarray *b)
{
	if (b) {
		free(b->p);
		b->p = NULL;
	}
}
