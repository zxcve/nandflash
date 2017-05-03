/**
 * Test program using the library to access the storage system
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kvlib.h"
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>

float getTimeDiff(struct timeval t0, struct timeval t1);
int entries = 65;
char key_A[4096], val_A[4096], keyChar[128], valChar[128], buffer[128];
char keyContainer[4096][256], valContainer[4096][256];	

void setMapping(char* key, char* val)
{
	if(key == NULL || val == NULL)
		return;
	int ret=0;
	ret = kvlib_set(key, val);
	if(ret != 0)
		printf("Error: in writing the key\t%s\n", key);
	return;
}

int getMapping(char* key, char* val)
{
	if(key == NULL || val == NULL)
		return -1;

	int ret=0;
	buffer[0]='\0';

	ret = kvlib_get(key, buffer);
	if(ret != 0)
	{
		printf("Error: in reading the key\t%s\n", key);
		return -1;
	}
	if(strcmp(buffer, val) != 0)
		printf("Error: returned value does not match the original value\n");
	return 0;
}

void deleteMapping(char* key)
{
	if(key == NULL)
		return;
	int ret = 0;
	buffer[0] = '\0';
	
	ret = kvlib_del(key);
	if(ret != 0)
		printf("Error: in deleting the key\t%s\n", key);
	if(kvlib_get(key, buffer) == 0)
		printf("Error: key not deleted properly:\t%s\n", key);
}
	
void smallKeyVal()
{
	int i, ret;
	struct timeval t0, t1;
	float timeDiff = 0;
	char keyContainer[4096][256], valContainer[4096][256];

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret = kvlib_format();
	if (ret == 0)
		printf("Formatting done:\n");
	else
		printf("Error in Formatting\n");
	
	
	for (i = 1; i < entries; i++) 
	{
		char key[128], val[128];
		sprintf(key, "key%d", i);
		sprintf(val, "val%d", i);
		strcpy(keyContainer[i], key);
		strcpy(valContainer[i], val);	
	}

	// Write latency to fill block with small keys
	gettimeofday(&t0,NULL);
	for (i = 0; i < entries; i++)
		setMapping(keyContainer[i], valContainer[i]);
	gettimeofday(&t1,NULL);

	timeDiff = getTimeDiff(t0,t1);
	printf("Write latency for small keys:\t %f \tus\n", timeDiff);
	
	// Read latency for small keys
	gettimeofday(&t0,NULL);
	for(i=1; i<entries; i++)
		getMapping(keyContainer[i], valContainer[i]);
	
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	printf("Read latency for small keys:\t %f \tus\n", timeDiff);

	// Update latency with small keys
	gettimeofday(&t0,NULL);
	for (i = 0; i < entries; i++)
		setMapping(keyContainer[i], strcat(valContainer[i], "update"));
	gettimeofday(&t1,NULL);

	timeDiff = getTimeDiff(t0,t1);
	printf("Update latency for small keys:\t %f \tus\n", timeDiff);
	
	// Delete latency for small keys
	gettimeofday(&t0,NULL);
	for(i=1; i<entries;i++)
		deleteMapping(keyContainer[i]);

	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Delete latency for small keys:\t %f \tus\n", timeDiff);
}

void largeKeyVal()
{
	int ret, i;
	struct timeval t0, t1;
	float timeDiff = 0;
	

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret=0;
	ret = kvlib_format();
	if (ret == 0)
		printf("Formatting done:\n");
	else
		printf("Error in Formatting\n");

	sprintf(key_A, "keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy%d", 0);
	sprintf(val_A, "valueeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee%d", 0);

	for (i = 1; i < entries; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		strcpy(keyContainer[i], key_A);
		strcpy(valContainer[i], val_A);
	}

	gettimeofday(&t0,NULL);
	for(i=1; i<entries; i++)
		setMapping(keyContainer[i], valContainer[i]);

	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Write latency for large keys:\t %f \tus\n", timeDiff);
	
	// Read latency for large keys
	gettimeofday(&t0,NULL);
	for(i=1; i<entries; i++)
		getMapping(keyContainer[i], valContainer[i]);

	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Read latency for large keys:\t %f\n", timeDiff);
	
	//Update latency of large keys
	gettimeofday(&t0,NULL);
	for (i = 0; i < entries; i++)
		setMapping(keyContainer[i], strcat(valContainer[i], "update"));
	gettimeofday(&t1,NULL);

	timeDiff = getTimeDiff(t0,t1);
	printf("Update latency for large keys:\t %f \tus\n", timeDiff);

	//Delete latency for large keys			
	gettimeofday(&t0,NULL);
	for (i = 1; i < entries; i++) 
		deleteMapping(keyContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Delete latency for large keys:\t %f \tus\n", timeDiff);
}

void varyingLenKeyVal()
{
	int ret, i;
	struct timeval t0, t1;
	float timeDiff = 0;

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret=0;
	ret = kvlib_format();
	if (ret == 0)
		printf("Formatting done:\n");
	else
		printf("Error in Formatting\n");

	// Stress testing: Varying the size of key and value
	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		
	
	for (i = 1; i < entries; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		strcpy(keyContainer[i], key_A);
		strcpy(valContainer[i], val_A);		
	}
	gettimeofday(&t0,NULL);
	for(i=1;i<entries; i++)
		setMapping(keyContainer[i], valContainer[i]);

	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);
	
	printf("Write latency for varying key/val:\t %f \tus\n", timeDiff);
	
	// Read latency for stress testing
	gettimeofday(&t0,NULL);
	for (i = 1; i < entries; i++) 
		getMapping(keyContainer[i], valContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Read latency for varying key/val:\t %f \tus\n", timeDiff);
	
	//Update latency of large keys
	gettimeofday(&t0,NULL);
	for (i = 0; i < entries; i++)
		setMapping(keyContainer[i], strcat(valContainer[i], "update"));
	gettimeofday(&t1,NULL);

	timeDiff = getTimeDiff(t0,t1);
	printf("Update latency for varying key/val:\t %f \tus\n", timeDiff);

	//Delete latency for stress testing
	gettimeofday(&t0,NULL);
	for (i = 1; i < entries; i++) 
		deleteMapping(keyContainer[i]);
	gettimeofday(&t1,NULL);
	timeDiff = getTimeDiff(t0,t1);

	printf("Delete latency for varying key/val:\t %f \tus\n", timeDiff);
}

void multipleRWD()
{
	int ret, i;
	
	buffer[0] = '\0';

	ret = 0;
	for (i=1; i< entries; i++)
	{
		sprintf(key_A, "key%d", i);
		ret += kvlib_get(key_A, buffer);
		ret += kvlib_del(key_A);
				
		sprintf(key_A, "key%d", i+entries);
		sprintf(val_A, "val%d", i*10);
		ret += kvlib_set(key_A, val_A);
	}
	printf(" ~~~ Multiple Read/Write testing ~~~\n");
	printf("returns: %d (should be 0)\n", ret);
	//@@@
}

int main(int argc, char *argv[])
{

	if(argc != 3)
	{
		printf("Error: Provide the valid arguments\n");
		printf("./testbench <key - val size> <number of entries>\n");
		printf("For Choose key - val size options as below\n");
		printf("1 for small keys/values test\n");
		printf("2 for large keys/values test\n");
		printf("3 for varying length of keys/values test\n");
		return 0;
	}

	int choice = atoi(argv[1]);
	entries = atoi(argv[2]) + 1;

	switch(choice)
	{
		case 1:
			printf("\nsmall keys/values test\n");
			smallKeyVal();
			break;
		case 2:
			printf("\nlarge keys/values test\n");
			largeKeyVal();
			break;
		case 3:
			printf("varying key/val length test\n");
			varyingLenKeyVal();
			break;
		default:
			printf("Error: Provide the valid arguments\n");
			printf("./testbench <key - val size> <number of entries>\n");
			printf("For Choose key - val size options as below\n");
			printf("1 for small keys/values test\n");
			printf("2 for large keys/values test\n");
			printf("3 for varying length of keys/values test\n");
			return 0;
	}
	return 0;

}

float getTimeDiff(struct timeval t0, struct timeval t1)
{
	return abs((t1.tv_usec - t0.tv_usec))/entries;	
}