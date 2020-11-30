/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// DefaultTokenTTL is the amount of time a bootstrap token (and therefore a KubeadmConfig) will be valid
	DefaultTokenTTL = 15 * time.Minute
)

// createToken attempts to create a token with the given ID.
func createToken(c client.Client) (string, error) {
	token, err := bootstraputil.GenerateBootstrapToken()
	if err != nil {
		return "", errors.Wrap(err, "unable to generate bootstrap token")
	}

	substrs := bootstraputil.BootstrapTokenRegexp.FindStringSubmatch(token)
	if len(substrs) != 3 {
		return "", errors.Errorf("the bootstrap token %q was not of the form %q", token, bootstrapapi.BootstrapTokenPattern)
	}
	tokenID := substrs[1]
	tokenSecret := substrs[2]

	secretName := bootstraputil.BootstrapTokenSecretName(tokenID)
	secretToken := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: metav1.NamespaceSystem,
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			bootstrapapi.BootstrapTokenIDKey:               []byte(tokenID),
			bootstrapapi.BootstrapTokenSecretKey:           []byte(tokenSecret),
			bootstrapapi.BootstrapTokenExpirationKey:       []byte(time.Now().UTC().Add(DefaultTokenTTL).Format(time.RFC3339)),
			bootstrapapi.BootstrapTokenUsageSigningKey:     []byte("true"),
			bootstrapapi.BootstrapTokenUsageAuthentication: []byte("true"),
			bootstrapapi.BootstrapTokenExtraGroupsKey:      []byte("system:bootstrappers:kubeadm:default-node-token"),
			bootstrapapi.BootstrapTokenDescriptionKey:      []byte("token generated by cluster-api-bootstrap-provider-kubeadm"),
		},
	}

	if err = c.Create(context.TODO(), secretToken); err != nil {
		return "", err
	}
	return token, nil
}

// getToken fetches the token Secret and returns an error if it is invalid.
func getToken(c client.Client, token string) (*v1.Secret, error) {
	substrs := bootstraputil.BootstrapTokenRegexp.FindStringSubmatch(token)
	if len(substrs) != 3 {
		return nil, errors.Errorf("the bootstrap token %q was not of the form %q", token, bootstrapapi.BootstrapTokenPattern)
	}
	tokenID := substrs[1]

	secretName := bootstraputil.BootstrapTokenSecretName(tokenID)
	secret := &v1.Secret{}
	if err := c.Get(context.TODO(), client.ObjectKey{Name: secretName, Namespace: metav1.NamespaceSystem}, secret); err != nil {
		return secret, err
	}

	if secret.Data == nil {
		return nil, errors.Errorf("Invalid bootstrap secret %q, remove the token from the kubadm config to re-create", secretName)
	}
	return secret, nil
}

// refreshToken extends the TTL for an existing token.
func refreshToken(c client.Client, token string) error {
	secret, err := getToken(c, token)
	if err != nil {
		return err
	}
	secret.Data[bootstrapapi.BootstrapTokenExpirationKey] = []byte(time.Now().UTC().Add(DefaultTokenTTL).Format(time.RFC3339))

	return c.Update(context.TODO(), secret)
}

// shouldRotate returns true if an existing token is past half of its TTL and should to be rotated.
func shouldRotate(c client.Client, token string) (bool, error) {
	secret, err := getToken(c, token)
	if err != nil {
		return false, err
	}

	expiration, err := time.Parse(time.RFC3339, string(secret.Data[bootstrapapi.BootstrapTokenExpirationKey]))
	if err != nil {
		return false, err
	}
	return expiration.Before(time.Now().UTC().Add(DefaultTokenTTL / 2)), nil
}
