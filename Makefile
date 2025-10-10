VERSION=0.2.7

check:
	go test -v .
	go test -race

bench:
	go test -bench . -benchmem ./...