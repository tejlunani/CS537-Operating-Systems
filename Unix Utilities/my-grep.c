#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main (int argc, char *argv[]) {
	if (argc == 1) {
		printf("my-grep: searchterm [file ...]\n");
		exit(1);
	}
	
	if (argc == 2) {
		char* line = NULL;
		size_t length = 0;
		while (!(getline(&line, &length, stdin) == -1)) {
			if (!(strstr(line, argv[1]) == NULL)) {
				printf("%s", line);
			}
		}
	}
	
	for (int i = 2; i < argc; i++) {
		char* line = NULL;
		size_t length = 0;
		FILE *fp = fopen(argv[i], "r");
		if (fp == NULL) {
			printf("my-grep: cannot open file\n");
			exit(1);
		}
		
		while (!(getline(&line, &length, fp) == -1)) {
			if (!(strstr(line, argv[1]) == NULL)) {
				printf("%s", line);
			}
		}
		fclose(fp);
	}
	exit(0);	
}
