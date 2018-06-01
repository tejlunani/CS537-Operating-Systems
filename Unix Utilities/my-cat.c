#include <stdio.h>
#include <stdlib.h>

int main (int argc, char *argv[]) {
	if (argc == 1) {
		exit(0);
	}
	
	for (int i = 1; i < argc; i++) {
		char buffer[200];
		FILE *fp = fopen(argv[i], "r");
		
		if (fp == NULL) {
			printf("my-cat: cannot open file\n");
			exit(1);
		}

		while(fgets(buffer, 200, fp)){
			printf("%s", buffer);
		}
		fclose(fp);
	}
	exit(0);
}
