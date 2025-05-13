package krtlite_test

import (
	krtlite "github.com/kalexmills/krt-lite"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

type keyerImpl string

func (k keyerImpl) Key() string {
	return string(k)
}

func TestGetKey(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		arg      any
	}{
		{
			name:     "string",
			expected: "foo",
			arg:      "foo",
		},
		{
			name:     "configmap",
			expected: "ns/a",
			arg: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "a"},
			},
		},
		{
			name:     "node",
			expected: "node-name",
			arg: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node-name"},
			},
		},
		{
			name:     "Keyer",
			expected: "fromKeyer",
			arg:      keyerImpl("fromKeyer"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				actual := krtlite.GetKey(tt.arg)
				assert.Equal(t, tt.expected, actual)
			})
		})
	}
}
