/**
 * Test program using the library to access the storage system
 */

#include <stdio.h>
#include <stdlib.h>
 #include <string.h>
/* Library header */
#include "kvlib.h"
#include <sys/time.h>

float getTimeDiff(struct timeval t0, struct timeval t1);

int main(void)
{
	int ret, i;
	char buffer[128];
	buffer[0] = '\0';
	struct timeval t0, t1;
	float timeDiff = 0;
	char key_A[4096], val_A[4096], keyChar[128], valChar[128];

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret = kvlib_format();
	printf("@@@ Formatting done:\n");
	// printf(" returns: %d (should be 0)\n", ret);

	/* "set" operation test */
	// ret = kvlib_set("key1", "val1");
	// printf("Insert 1 (key1, val1):\n");
	// printf(" returns: %d (should be 0)\n", ret);

	// ret = kvlib_set("key2", "val2");
	// printf("Insert 2 (key2, val2):\n");
	// printf(" returns: %d (should be 0)\n", ret);

	/* Now let's fill an entire block lplus an additional page (we assume 
	 * there are 64 pages per block) */


	// Write latency to fill a block with small keys
	ret = 0;
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		char key[128], val[128];
		sprintf(key, "key%d", i);
		sprintf(val, "val%d", i);
		ret += kvlib_set(key, val);
	}
	gettimeofday(&t1,NULL);
	
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Write latency for small keys:\t %f \tus\n", timeDiff);
	// printf(" returns: %d (should be 0)\n", ret);


	// Read latency for small keys

	ret = 0;
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) {
		char key[128];
		sprintf(key, "key%d", i);
		ret += kvlib_get(key, buffer);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Read latency for small keys:\t %f \tus\n", timeDiff);
	// printf(" returns: %d (should be 0)\n", ret);

	// Delete latency for small keys
	ret = 0;
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) {
		char key[128];
		sprintf(key, "key%d", i);
		ret += kvlib_del(key);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Delete latency for small keys:\t %f \tus\n", timeDiff);
	// printf(" returns: %d (should be 0)\n", ret);


	// @@@ Large keys:
	ret = 0;
	sprintf(key_A, "keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy%d", 0);
	sprintf(val_A, "valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee%d", 0);

	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		ret += kvlib_set(key_A, val_A);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Write latency for large keys:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);

	// Read latency for stress testing

	ret = 0;
	//char key_A[128], val_A[128], keyChar[128], valChar[128];

	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		strcat(key_A, keyChar);
		ret += kvlib_get(key_A, buffer);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Read latency for large keys:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);

	//Delete latency for stress testing
	ret = 0;
	//char key_A[128], val_A[128], keyChar[128], valChar[128];

	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		strcat(key_A, keyChar);
		ret += kvlib_del(key_A);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Delete latency for large keys:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);


	// @@@ Stress testing: Varying the size of key and value
	ret = 0;
	
	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 500; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		//printf("@@@ key inserted : %s\n", key_A);
		ret += kvlib_set(key_A, val_A);
		//printf("@@@ returns: %d (should be 0)\n", ret);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Write latency for varying key/val:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);

	// Read latency for stress testing

	ret = 0;
	//char key_A[128], val_A[128], keyChar[128], valChar[128];

	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		strcat(key_A, keyChar);
		ret += kvlib_get(key_A, buffer);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Read latency for varying key/val:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);

	//Delete latency for stress testing

	ret = 0;
	//char key_A[128], val_A[128], keyChar[128], valChar[128];

	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		strcat(key_A, keyChar);
		ret += kvlib_del(key_A);
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("@@@ Delete latency for varying key/val:\t %f \tus\n", timeDiff);
	// printf("returns: %d (should be 0)\n", ret);

	// Simultaneous read/write/delete
	// Read from key0 to key64 and write to key64 to key128
	ret = 0;
	for (i=0; i< 65; i++)
	{
		sprintf(key_A, "key%d", i);
		ret += kvlib_get(key_A, buffer);
		ret += kvlib_del(key_A);
				
		sprintf(key_A, "key%d", i+65);
		sprintf(val_A, "val%d", i*10);
		ret += kvlib_set(key_A, val_A);
	}
	printf(" ~~~ Multiple Read/Write testing ~~~\n");
	printf("returns: %d (should be 0)\n", ret);
	//@@@
	return EXIT_SUCCESS;

}

float getTimeDiff(struct timeval t0, struct timeval t1)
{
	return abs((t1.tv_usec - t0.tv_usec));	
}