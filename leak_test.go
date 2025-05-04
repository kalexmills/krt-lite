package krtlite

import (
	"istio.io/istio/tests/util/leak"
	"testing"
)

func TestMain(m *testing.M) {
	// Bash one-liner to help narrow down test failures:
	// go test -c -o tests && for test in $(go test -list . | grep -E "^(Test|Example)"); do ./tests -test.run "^$test\$" &>/dev/null && echo -n "." || echo -e "\n$test failed"; done
	leak.CheckMain(m)
}
