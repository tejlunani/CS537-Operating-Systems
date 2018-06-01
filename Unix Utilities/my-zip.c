#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
	if (argc == 1) {
		printf("my-zip: file1 [file2 ...]\n");
		exit(1);
	}
	
	FILE *file = tmpfile();
	
	for (int i = 1; i < argc; i++) {
		char c;
		FILE *fp = fopen(argv[i], "r");
		if (fp == NULL) {
			printf("my-zip: cannot open file\n");
			exit(1);
		}
		
		while ((c = fgetc(fp)) != EOF) {
			fputc(c, file);
		}
		fclose(fp);
	}
	
	rewind(file);	
	int prev = fgetc(file);
	int curr;
	int count = 1;
	while (!(feof(file))) {
		curr = fgetc(file);
		if (prev == curr) {
			count++;
		} else {
			fwrite(&count, sizeof(int), 1, stdout);
			printf("%c", prev);
			count = 1;
			prev = curr;
		}
	}
	
	fclose(file);
	exit(0);
}
