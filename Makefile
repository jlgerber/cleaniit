
build:
	cargo build --release

install:
	cp target/release/cleaniit ~/bin/.

all: build install
