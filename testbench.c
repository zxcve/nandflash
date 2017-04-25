/**
 * Test program using the library to access the storage system
 */

#include <stdio.h>
#include <stdlib.h>
 #include <string.h>
/* Library header */
#include "kvlib.h"

int main(void)
{
	int ret, i;
	char buffer[128];
	buffer[0] = '\0';

	/* first let's format the partition to make sure we operate on a 
	 * well-known state */
	ret = kvlib_format();
	printf("Formatting done:\n");
	printf(" returns: %d (should be 0)\n", ret);

	/* "set" operation test */
	ret = kvlib_set("key1", "val1");
	printf("Insert 1 (key1, val1):\n");
	printf(" returns: %d (should be 0)\n", ret);

	ret = kvlib_set("key2", "val2");
	printf("Insert 2 (key2, val2):\n");
	printf(" returns: %d (should be 0)\n", ret);

	/* Now let's fill an entire block lplus an additional page (we assume 
	 * there are 64 pages per block) */
	ret = 0;
	for (i = 3; i < 65; i++) {
		char key[128], val[128];
		sprintf(key, "key%d", i);
		sprintf(val, "val%d", i);
		ret += kvlib_set(key, val);
	}
	printf("Insert 3->65:\n");
	printf(" returns: %d (should be 0)\n", ret);


	// @@@ Stress testing: Varying the size of key and value
	ret = 0;
	char key_A[128], val_A[128], keyChar[128], valChar[128];

	sprintf(key_A, "key%d", 0);
	sprintf(val_A, "val%d", 0);
		

	for (i = 3; i < 64; i++) 
	{
		sprintf(keyChar, "%d", i);
		sprintf(valChar, "%d", i);
		strcat(key_A, keyChar);
		strcat(val_A, valChar);
		//printf("@@@ key inserted : %s\n", key_A);
		ret += kvlib_set(key_A, val_A);
		//printf("@@@ returns: %d (should be 0)\n", ret);
	}
	printf(" ~~~ Stress Testing ~~~\n");
	printf("returns: %d (should be 0)\n", ret);

	// Simultaneous read/write
	// Read from key0 to key64 and write to key64 to key128
	ret = 0;
	for (i=0; i< 65; i++)
	{
		sprintf(key_A, "key%d", i);
		ret += kvlib_get("key1", buffer);
				
		sprintf(key_A, "key%d", i+65);
		sprintf(val_A, "val%d", i*10);
		ret += kvlib_set(key_A, val_A);
	}
	printf(" ~~~ Multiple Read/Write testing ~~~\n");
	printf("returns: %d (should be 0)\n", ret);
	//@@@

	/* "get" operation test */
	ret = kvlib_get("key1", buffer);
	printf("Reading the value of key1:\n");
	printf(" returns: %d (should be 0), read: %s (should be val1)\n", ret,
		buffer);

	ret = kvlib_get("key35", buffer);
	printf("Reading the value of key35:\n");
	printf(" returns: %d (should be 0), read: %s (should be val35)\n", ret,
		buffer);

	/* trying to set a value for an already existing key */
	ret = kvlib_set("key1", "crazyval");
	printf("Trying to insert an already existing key:\n");
	printf(" returns: %d (should be -4)\n", ret);

	/* trying to get the value of a non-existing key */
	ret = kvlib_get("key2000", buffer);
	printf("Trying to get the value of a non-existing key:\n");
	printf(" returns: %d (should be -3)\n", ret);

	/* Let's format again before we fill all the flash */
	ret = kvlib_format();
	printf("Formatting:\n");
	printf(" returns: %d (should be 0)\n", ret);

	/* Fill the flash completely, note that we assume here a small partition
	 * of 10 blocks, each containing 64 pages */
	printf ("Insertion 0->639 (flash should be full after that)"
		", may take some time...\n");
	fflush(stdout);
	ret = 0;
	for (i = 0; i < 639; i++) {
		char key[128], val[128];
		sprintf(key, "key%d", i);
		sprintf(val, "val%d", i);
		ret += kvlib_set(key, val);
	}
	printf(" returns: %d (should be 0)\n", ret);

	/* The flash is full and the system should be read-only, let's try to
	 * add an additional key/value: */
	ret = kvlib_set("key640", "val640");
	printf("Trying to insert another key/val:\n");
	printf(" returns: %d (should be -5)\n", ret);

	/* Format again */
	ret = kvlib_format();
	printf("Formatting:\n");
	printf(" returns: %d (should be 0)\n", ret);

	/* Let's try to add a key/value again: */
	ret = kvlib_set("key640", "val640");
	printf("Insert a key/val after formatting:\n");
	printf(" returns: %d (should be 0)\n", ret);

	/* get the value we just inserted */
	ret = kvlib_get("key640", buffer);
	printf("Reading the val of key640:\n");
	printf(" returns: %d, read: %s (should be val640)\n", ret, buffer);

	return EXIT_SUCCESS;

}



// /**
//  * Test program using the library to access the storage system
//  */

// #include <stdio.h>
// #include <stdlib.h>
// /* Library header */
// #include "kvlib.h"

// char buffer[4096] = "123wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";

// char value[4096] = "567aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb89";


// char value2[4096] = "567XDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb89RR";

// char tmpbuf[4096];

// int main(void)
// {
// 	int ret;
// 	tmpbuf[0] = '\0';

// 	/* first let's format the partition to make sure we operate on a 
// 	 * well-known state */
// 	ret = kvlib_format();
// 	printf("Formatting done:\n");
// 	printf(" returns: %d (should be 0)\n", ret);

// 	/* "set" operation test */
// 	ret = kvlib_set("key1", value);
// 	printf("Insert 1 (key1):\n");
// 	printf(" returns: %d (should be 0)\n", ret);

// 	/* "set" operation test */
// 	ret = kvlib_set("key2", value2);
// 	printf("Insert 1 (key2):\n");
// 	printf(" returns: %d (should be 0)\n", ret);

// 	ret = kvlib_get("key1", tmpbuf);
// 	printf("Reading the value of key1:\n");
// 	printf(" returns: %d (should be 0), read: %s\n", ret,
// 		tmpbuf);

// 	ret = kvlib_get("key2", tmpbuf);
// 	printf("Reading the value of key2:\n");
// 	printf(" returns: %d (should be 0), read: %s\n", ret,
// 		tmpbuf);

// 	/* "set" operation test */
// 	ret = kvlib_set("key2", value);
// 	printf("Insert 1 (key1, val1):\n");
// 	printf(" returns: %d (should be 0)\n", ret);
// 	ret = kvlib_get("key2", tmpbuf);
// 	printf("Reading the value of key2:\n");
// 	printf(" returns: %d (should be 0), read: %s\n", ret,
// 		tmpbuf);

// 	ret = kvlib_del("key2");
// 	printf("Deleting the value of key2: %d\n", ret);

// 	ret = kvlib_get("key2", tmpbuf);
// 	printf("Reading the value of key2:\n");
// 	printf(" returns: %d (should be 0), read: %s\n", ret,
// 		tmpbuf);

// 	return EXIT_SUCCESS;
// }
