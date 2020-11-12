ifdef update
  u=-u
endif

VERSION=0.0.6
LDFLAGS=-ldflags "-X main.Version=${VERSION}"
GO111MODULE=on


.PHONY: deps

deps:
	go get ${u} -d
	go mod tidy

tag:
	git tag v${VERSION}
	git push origin v${VERSION}
	git push origin master
