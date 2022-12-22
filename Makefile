ifdef update
  u=-u
endif

VERSION=0.1.0
LDFLAGS=-ldflags "-X main.Version=${VERSION}"
GO111MODULE=on

tag:
	git tag v${VERSION}
	git push origin v${VERSION}
	git push origin main

check:
	go test .