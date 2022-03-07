package rkredis

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
	"time"
)

func TestGetRedisEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetRedisEntry("not-exist"))

	// happy case
	entry := RegisterRedisEntry()
	assert.Equal(t, entry, GetRedisEntry(entry.GetName()))
	entry.Interrupt(context.TODO())
}

func TestRegisterRedisEntryFromConfig(t *testing.T) {
	bootConfigStr := `
redis:
  - name: ut-redis
    enabled: true
    addrs: ["localhost:3306"]
`

	entries := RegisterRedisEntryYAML([]byte(bootConfigStr))

	assert.NotEmpty(t, entries)

	entry := entries["ut-redis"]
	assert.Equal(t, "ut-redis", entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())

	rkentry.GlobalAppCtx.RemoveEntry(entry)
}

func TestRedisEntry_Bootstrap(t *testing.T) {
	defer assertNotPanic(t)

	// single
	entry := RegisterRedisEntry()
	entry.Bootstrap(context.TODO())

	client, ok := entry.GetClient()
	assert.NotNil(t, client)
	assert.True(t, ok)

	cluster, ok := entry.GetClientCluster()
	assert.Nil(t, cluster)
	assert.False(t, ok)

	entry.Interrupt(context.TODO())

	// sentinel
	entry = RegisterRedisEntry()
	entry.Opts.MasterName = "localhost:6379"
	entry.Bootstrap(context.TODO())

	client, ok = entry.GetClient()
	assert.NotNil(t, client)
	assert.True(t, ok)

	cluster, ok = entry.GetClientCluster()
	assert.Nil(t, cluster)
	assert.False(t, ok)

	entry.Interrupt(context.TODO())

	// cluster
	entry = RegisterRedisEntry()
	entry.Opts.Addrs = append(entry.Opts.Addrs, "localhost:6379")
	entry.Bootstrap(context.TODO())

	client, ok = entry.GetClient()
	assert.Nil(t, client)
	assert.False(t, ok)

	cluster, ok = entry.GetClientCluster()
	assert.NotNil(t, cluster)
	assert.True(t, ok)

	entry.Interrupt(context.TODO())
}

func generateCerts() ([]byte, []byte) {
	// Create certs and return as []byte
	ca := &x509.Certificate{
		Subject: pkix.Name{
			Organization: []string{"Fake cert."},
		},
		SerialNumber:          big.NewInt(42),
		NotAfter:              time.Now().Add(2 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create a Private Key
	key, _ := rsa.GenerateKey(rand.Reader, 4096)

	// Use CA Cert to sign a CSR and create a Public Cert
	csr := &key.PublicKey
	cert, _ := x509.CreateCertificate(rand.Reader, ca, ca, csr, key)

	// Convert keys into pem.Block
	c := &pem.Block{Type: "CERTIFICATE", Bytes: cert}
	k := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}

	return pem.EncodeToMemory(c), pem.EncodeToMemory(k)
}

func assertNotPanic(t *testing.T) {
	if r := recover(); r != nil {
		// Expect panic to be called with non nil error
		assert.True(t, false)
	} else {
		// This should never be called in case of a bug
		assert.True(t, true)
	}
}
