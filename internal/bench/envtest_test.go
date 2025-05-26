package bench_test

import (
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func NewEnvtest() (*envtest.Environment, *rest.Config, error) {
	env := &envtest.Environment{
		AttachControlPlaneOutput: true,
	}

	cfg, err := env.Start()
	return env, cfg, err
}
