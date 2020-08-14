export RUSTFLAGS = -C prefer-dynamic -C link-args=-Wl,-rpath,/home/kogia-sima/.rustup/toolchains/beta-x86_64-unknown-linux-gnu/lib/ -C lto=no -C debuginfo=0 -C debug-assertions=no

all: release

build:
	cargo build

run:
	cargo run

release:
	cargo rustc --release -- -C target-cpu=native
