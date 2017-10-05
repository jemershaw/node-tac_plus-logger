#
# Tools
#
TAPE := ./node_modules/.bin/tape
NPM			 := npm
.PHONY: all
all: 
.PHONY: test
test: $(TAPE)
	mkdir -p tmp
	find test/ -name '*.test.js' | xargs -n 1 $(TAPE)

.PHONY: clean
clean:
	rm -r tmp
