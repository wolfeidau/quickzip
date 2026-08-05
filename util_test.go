package quickzip

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithinChroot(t *testing.T) {
	sep := string(filepath.Separator)
	p := func(parts ...string) string { return sep + strings.Join(parts, sep) }

	tests := []struct {
		path   string
		chroot string
		want   bool
	}{
		{p("home", "user", "file"), p("home", "user"), true},
		{p("home", "user"), p("home", "user"), true},
		{p("home", "user-evil", "file"), p("home", "user"), false},
		{p("etc", "passwd"), p("home", "user"), false},
		{p("home", "user", "file"), sep, true},
		{sep, sep, true},
	}

	for _, test := range tests {
		assert.Equal(t, test.want, withinChroot(test.path, test.chroot),
			"withinChroot(%q, %q)", test.path, test.chroot)
	}
}
