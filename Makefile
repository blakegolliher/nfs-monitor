BINARY  := nfs-monitor
PREFIX  := /usr/local
DESTDIR :=

GO      ?= go
GOFLAGS ?=

.PHONY: all build install uninstall test vet fmt clean

all: build

build:
	$(GO) build $(GOFLAGS) -o $(BINARY) .

install: build
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(BINARY) $(DESTDIR)$(PREFIX)/bin/$(BINARY)
	@echo "Installed to $(DESTDIR)$(PREFIX)/bin/$(BINARY)"

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(BINARY)

test:
	$(GO) test $(GOFLAGS) ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -f $(BINARY)
