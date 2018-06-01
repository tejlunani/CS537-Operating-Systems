#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include "fs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "types.h"


void *iptr;
uchar *bitmap;
struct superblock *sb;
struct dinode *dp;

int main(int argc, char *argv[]) {
	int n = argc;
	if (n != 2) {
		fprintf(stderr, "Usage: xcheck <file_system_image>\n");
		exit(1);
	}
	
	char* fname = argv[1];
	int fd = open(fname, O_RDONLY);
	if (fd == -1) {
		fprintf(stderr, "image not found.\n");
		exit(1);
	}
	
	struct stat buf;
	int rc = fstat(fd, &buf);
	assert(rc == 0);
	iptr = mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	assert (iptr != MAP_FAILED);	
	sb = (struct superblock *)(iptr + BSIZE);
	dp = (struct dinode *)(iptr + 2*BSIZE);
	
	int dperblock = (BSIZE / sizeof(struct dirent));
	int const size = (int) sb->size;
	uint numinodes = (sb->ninodes + IPB - 1) / IPB;
	uint numbitmaps = (sb->ninodes + BPB - 1) / BPB;
	bitmap = (uchar*)(iptr+BSIZE*((numinodes) + 3));
	int use[size];
	for (int i = 0; i < size; i++) {
		use[i] = 0;
	}

	////////////////////////////////////////////////////////////////////////

	uint bound = numbitmaps + numinodes + 3;
	uint addr;
	uint iaddr;
	uint* indir;
	struct dirent* directs;
	uint loc = 0;
	uint offset = 0;

	////////////////////////////////////////////////////////////////////////

	for (int i = 1; i < (int) sb->ninodes; i++) {
		if (!(dp[i].type == 0 || 
		      dp[i].type == 1 || 
		      dp[i].type == 2 || 
		      dp[i].type == 3)) {
			fprintf(stderr, "ERROR: bad inode.\n");
			exit(1);
		}
		if (dp[i].type == 1) {
			addr = dp[i].addrs[0];
			directs = (struct dirent *) (iptr + BSIZE*addr);
			if (!(strcmp(directs[0].name, ".") == 0 &&
			      strcmp(directs[1].name, "..") == 0 )) {
				//might need to check if "." entry points to directory itself
				fprintf(stderr, "ERROR: directory not properly formatted.\n");
				exit(1);
			}
		}


//check if the address == 0 - NOT ERROR
		for (int j = 0; j < NDIRECT; j++) {
			if (dp[i].addrs[j] == 0) { }
			else if (dp[i].addrs[j] >= size || dp[i].addrs[j] < bound) {
				fprintf(stderr, "ERROR: bad direct address in inode.\n");
				exit(1);
			}
		}		

		addr = dp[i].addrs[NDIRECT];
		if (addr !=0) {
			if (addr >= size || addr < bound) {
				fprintf(stderr, "ERROR: bad indirect address in inode.\n");
				exit(1);
			}
			indir = (uint*) (iptr + BSIZE*addr);
			for (int j = 0; j < NINDIRECT; j++) {
				iaddr = indir[j];
				if (iaddr != 0 && (iaddr > size || iaddr < bound)) {
					fprintf(stderr, "ERROR: bad indirect address in inode.\n");
                                	exit(1);
				}
			}
		}
		if (dp[i].type != 0) {
			for (int j = 0; j < NDIRECT; j++) {
				addr = dp[i].addrs[j];
				if (addr != 0) {
					if (use[addr] == 0) { use[addr] = 1; }
					else { 
						fprintf(stderr, "ERROR: direct address used more than once.\n");
						exit(1);
					}
				}	
			}
			if (dp[i].addrs[NDIRECT] != 0) {
				addr = dp[i].addrs[NDIRECT];
				if (use[addr] == 0) { use[addr] = 1; }
				else { 
					fprintf(stderr, "ERROR: direct address used more than once.\n");
					exit(1);
				}
				indir = (uint*)(iptr + BSIZE*addr);
				for (int j = 0; j < NINDIRECT; ++j) {
					iaddr = indir[j];
					if (iaddr != 0) {
						if (use[iaddr] == 0) { use[iaddr] = 1; }
						else {
							fprintf(stderr, "ERROR: indirect address used more than once.\n");
							exit(1);
						}						
					}
				}
			}
			for (uint j = 0; j < NDIRECT; ++j) {
				addr = dp[i].addrs[j];
				loc = addr / 8;
				offset = addr % 8;
				//potentially investigate
				if (! ((1<<offset)& bitmap[loc]) ) {
					fprintf(stderr,"ERROR: address used by inode but marked free in bitmap.\n");
					exit(1);
				}
			}
			if (dp[i].addrs[NDIRECT] != 0) {
				addr = dp[i].addrs[NDIRECT];
				indir = (uint*)(iptr + BSIZE*addr);
				for (uint j = 0; j < NINDIRECT; ++j) {
					iaddr = indir[j];
					if (iaddr != 0) {
						loc = iaddr / 8;
						offset = iaddr % 8;
						if (! ((1<<offset)& bitmap[loc]) ) {
							fprintf(stderr,"ERROR: address used by inode but marked free in bitmap.\n");
							exit(1);
						}
					}
				}
			}
		}
	}
	
	addr = dp[1].addrs[0];
	directs = (struct dirent *)(iptr + BSIZE*addr);
	if (dp[1].type != 1) {
		fprintf(stderr,"ERROR: root directory does not exist.\n");
		exit(1);
	}
	if( !(strcmp(directs[0].name, ".") == 0 && 
	      directs[0].inum == 1 &&
	      strcmp(directs[1].name, "..") == 0 &&
	      directs[1].inum == 1) ) {
		fprintf(stderr,"ERROR: root directory does not exist.\n");
		exit(1);
	}

	////////////////////////////////////////////////////////////////////////

	ushort ref[(int) sb->ninodes];
	memset(ref, 0, sizeof(ref));
	ref[1] = 1;
	ushort parent[(int)sb->ninodes];
	memset(parent, 0, sizeof(parent));
	ushort child[(int)sb->ninodes];
        memset(child, 0, sizeof(child));
	child[1] = 1;

	////////////////////////////////////////////////////////////////////////

	for (int i=0; i < (int)sb->ninodes; i++) {
		if (dp[i].type == 1) {
			for (uint j = 0; j < NDIRECT; ++j) {
				addr = dp[i].addrs[j];
				directs = (struct dirent *) (iptr + BSIZE*addr);
				int x = 0;
				if (j == 0) {
					parent[i] = directs[1].inum;
					x = 2;
				}
				for (; x < dperblock; x++) {
					if (directs[x].inum != 0) {
						ref[directs[x].inum]++;
						child[directs[x].inum] = i;
					}
				}
			}
			if (dp[i].addrs[NDIRECT] != 0) {
				addr = dp[i].addrs[NDIRECT];
				indir = (uint *) (iptr + BSIZE*addr);
				for (uint j = 0; j < NINDIRECT; ++j) {
					iaddr = indir[j];
					if (iaddr != 0) {
						directs = (struct dirent *) (iptr + BSIZE*iaddr);
						for (int k = 0; k < dperblock; ++k) {
							if (directs[k].inum != 0) {
								ref[directs[k].inum]++;
								child[directs[k].inum] = i;
							}
						}
					}
				}
			}
		}
	}
	for (int i = 0; i < (int)sb->ninodes; i++) {
		if (dp[i].type != 0 && ref[i] == 0) {
			fprintf(stderr, "ERROR: inode marked use but not found in a directory.\n");
			exit(1);
		}
		if (dp[i].type == 0 && ref[i] > 0) {
			fprintf(stderr, "ERROR: inode referred to in directory but marked free.\n");
			exit(1);
		}

		if (dp[i].type == 1 && ref[i] > 1) {
			fprintf(stderr, "ERROR: directory appears more than once in file system.\n");
			exit(1);
		}

		if (dp[i].type == 1 && parent[i] != child[i]) {
			fprintf(stderr, "ERROR: parent directory mismatch.\n");
			exit(1);
		}
		if (dp[i].type == 2 && dp[i].nlink!=ref[i]) {
			fprintf(stderr, "ERROR: bad reference count for file.\n");
			exit(1);
		}
	}

	for (int i = (numinodes + 4); i < size; i++) {
		loc = i / 8;
		offset = i % 8;
		if (use[i] == 0 && (bitmap[loc]&(1<<offset))) {
			fprintf(stderr,"ERROR: bitmap marks block in use but it is not in use.\n");
			exit(1);
		}
	}
	
	return 0;
}
