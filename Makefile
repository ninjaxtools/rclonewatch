GO ?= go
GOFLAGS ?=
PREFIX ?= $(HOME)/.local
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)
ARCHIVE = rclonewatch_$(GOOS)_$(GOARCH).tar.gz

.PHONY: build test check install release release-platform clean

build:
	mkdir -p target
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -ldflags='-s -w' -o target/.rclonewatch-build ./src
	mv -f target/.rclonewatch-build target/rclonewatch

test:
	$(GO) test $(GOFLAGS) ./...

check: test
	$(GO) vet $(GOFLAGS) ./...

install: build
	install -d "$(PREFIX)/bin"
	ln -sfn "$(CURDIR)/target/rclonewatch" "$(PREFIX)/bin/rclonewatch"

release:
	@for arch in amd64 arm64; do \
		$(MAKE) release-platform GOOS=linux GOARCH=$$arch || exit; \
	done

release-platform:
	mkdir -p target/staging/$(GOOS)-$(GOARCH)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GO) build $(GOFLAGS) -trimpath -ldflags='-s -w' -o target/rclonewatch-$(GOOS)-$(GOARCH) ./src
	cp target/rclonewatch-$(GOOS)-$(GOARCH) target/staging/$(GOOS)-$(GOARCH)/rclonewatch
	tar -C target/staging/$(GOOS)-$(GOARCH) -czf target/$(ARCHIVE) rclonewatch
	cd target && shasum -a 256 $(ARCHIVE) > $(ARCHIVE).sha256

clean:
	rm -rf target
