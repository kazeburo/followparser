ifdef update
  u=-u
endif

VERSION=0.2.2
LDFLAGS=-ldflags "-X main.Version=${VERSION}"
GO111MODULE=on

tag:
	git tag v${VERSION}
	git push origin v${VERSION}
	git push origin master

check:
	go test -v .
	go test -race
