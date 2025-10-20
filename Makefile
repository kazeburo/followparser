VERSION=0.2.9

check:
	go test -v .
	go test -race

bench:
	go test -bench . -benchmem ./...