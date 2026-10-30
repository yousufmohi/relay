package internal

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sScaler scales a Deployment through its /scale subresource. Scale-down
// relies on the worker pods' preStop hook (sidecar /drain) so in-flight
// streams finish before the model server stops.
type K8sScaler struct {
	Client     kubernetes.Interface
	Namespace  string
	Deployment string
}

// NewK8sScaler uses in-cluster config, or KUBECONFIG when run outside.
func NewK8sScaler(namespace, deployment, kubeconfig string) (*K8sScaler, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &K8sScaler{Client: cs, Namespace: namespace, Deployment: deployment}, nil
}

func (k *K8sScaler) Current(ctx context.Context) (int, error) {
	s, err := k.Client.AppsV1().Deployments(k.Namespace).GetScale(ctx, k.Deployment, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	return int(s.Spec.Replicas), nil
}

func (k *K8sScaler) Scale(ctx context.Context, n int) error {
	s, err := k.Client.AppsV1().Deployments(k.Namespace).GetScale(ctx, k.Deployment, metav1.GetOptions{})
	if err != nil {
		return err
	}
	s.Spec.Replicas = int32(n)
	_, err = k.Client.AppsV1().Deployments(k.Namespace).UpdateScale(ctx, k.Deployment, s, metav1.UpdateOptions{})
	return err
}
