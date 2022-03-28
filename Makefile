PROG=low-runner
LDFLAGS=-ldflags="-s -w"

all: $(PROG)

$(PROG): *.go
	CGO_ENABLED=0 go build $(LDFLAGS) .

install: $(PROG)
	CGO_ENABLED=0 go install $(LDFLAGS) .

clean:
	-rm $(PROG)

.PHONY: all install clean
