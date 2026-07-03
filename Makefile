VERSION=0.2.11

check:
	go test -v .
	go test -race

bench:
	go test -bench . -benchmem ./...