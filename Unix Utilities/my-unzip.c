#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
	if (argc == 1) {
		printf("my-unzip: file1 [file2 ...]\n");
		exit(1);
	}
	
	char buf[2048];
	int ch;
	for (int i = 1; i < argc; i++) {
		FILE *fp = fopen(argv[i], "r");
		
		if (fp == NULL) {
			printf("my-unzip: cannot open file\n");
			exit(1);
		}
		
		while (fread(buf, sizeof(int), 1, fp) == 1) {
			int c = buf[0] + 256 * buf[1];
			fread(&ch, sizeof(char), 1, fp);
			for (int i = 0; i < c; i++) {
				printf("%c", ch);
			}
		}
		fclose(fp);
	}
	exit(0);	
}

