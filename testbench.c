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
	int ret, i, misMatch;
	char buffer[128];
	buffer[0] = '\0';
	struct timeval t0, t1;
	float timeDiff = 0;
	char key_A[4096], val_A[4096], keyChar[128], valChar[128];
	char keyContainer[4096][256], valContainer[4096][256];

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret = kvlib_format();
	if (ret == 0)
		printf("Formatting done:\n");
	else
		printf("Error in Formatting\n");
	
	// Write latency to fill a block with small keys

	ret=0;
	
	for (i = 1; i < 65; i++) 
	{
		char key[128], val[128];
		sprintf(key, "key%d", i);
		sprintf(val, "val%d", i);
		strcpy(keyContainer[i], key);
		strcpy(valContainer[i], val);	
	}

	gettimeofday(&t0,NULL);
	for (i = 0; i < 65; i++)
		ret += kvlib_set(keyContainer[i], valContainer[i]);
	gettimeofday(&t1,NULL);

	timeDiff = getTimeDiff(t0,t1);
	if (ret == 0)
		printf("Write latency for small keys:\t %f \tus\n", timeDiff);
	else
		printf("Error in writing small keys...\n");
	
	
	// Read latency for small keys

	ret = 0;
	misMatch = 0;

	gettimeofday(&t0,NULL);
	for(i=1; i<65; i++)
	{
		ret += kvlib_get(keyContainer[i], buffer);
		if(strcmp(buffer, valContainer[i]) != 0)
			misMatch++;
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	
	if (ret == 0)
		printf("Read latency for small keys:\t %f \tus \t misMatch: %d\n", timeDiff, misMatch);
	else
		printf("Error in reading small keys...\n");
	
	// Delete latency for small keys
	ret = 0;

	gettimeofday(&t0,NULL);
	for(i=1; i<65;i++)
		ret += kvlib_del(keyContainer[i]);
	
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Delete latency for small keys:\t %f \tus\n", timeDiff);
	else
		printf("Error in Deleting small keys...\n");

	// @@@ Writing Large keys:
	ret = 0;
	sprintf(key_A, "keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy%d", 0);
	sprintf(val_A, "valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee%d", 0);

	for (i = 1; i < 65; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		strcpy(keyContainer[i], key_A);
		strcpy(valContainer[i], val_A);
	}

	gettimeofday(&t0,NULL);
	for(i=1; i<65; i++)
		ret += kvlib_set(keyContainer[i], valContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Write latency for large keys:\t %f \tus\n", timeDiff);
	else
		printf("Error in writing large keys...\n");

	// Read latency for large keys
	ret = 0;
	misMatch = 0;

	gettimeofday(&t0,NULL);
	for(i=1; i<65; i++)
	{
		ret += kvlib_get(keyContainer[i], buffer);
		if(strcmp(buffer, valContainer[i]) != 0)
			misMatch++;
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Read latency for large keys:\t %f \tus \t misMatch: %d\n", timeDiff, misMatch);
	else
		printf("Error in reading large keys...\n");


	//Delete latency for large keys
	ret = 0;
		
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
		ret += kvlib_del(keyContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Delete latency for large keys:\t %f \tus\n", timeDiff);
	else
		printf("Error in deleting large keys...\n");
	
	// @@@ Stress testing: Varying the size of key and value
	ret = 0;
	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	
	for (i = 1; i < 500; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		strcpy(keyContainer[i], key_A);
		strcpy(valContainer[i], val_A);		
	}
	gettimeofday(&t0,NULL);
	for(i=1;i<65; i++)
		ret += kvlib_set(keyContainer[i], valContainer[i]);	

	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	
	if (ret == 0)
		printf("Write latency for varying key/val:\t %f \tus\n", timeDiff);
	else
		printf("Error in writing varying keys...\n");

	// Read latency for stress testing
	ret = 0;
	misMatch=0;

	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
	{
		ret += kvlib_get(keyContainer[i], buffer);
		if(strcmp(buffer, valContainer[i]) != 0)
			misMatch++;
	}
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Read latency for varying key/val:\t %f \tus \t misMatch: %d\n", timeDiff, misMatch);
	else
		printf("Error in reading varying keys...\n");

	//Delete latency for stress testing
	ret = 0;
	gettimeofday(&t0,NULL);
	for (i = 1; i < 65; i++) 
		ret += kvlib_del(keyContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	if (ret == 0)
		printf("Delete latency for varying key/val:\t %f \tus\n", timeDiff);
	else
		printf("Error in deleting varying keys...\n");


	//Let's format again...
	ret = kvlib_format();
	if (ret == 0)
		printf("Formatting done:\n");
	else
		printf("Error in Formatting\n");

	// Simultaneous read/write/delete
	// Read from key0 to key64 and write to key64 to key128
	ret = 0;
	for (i=1; i< 65; i++)
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