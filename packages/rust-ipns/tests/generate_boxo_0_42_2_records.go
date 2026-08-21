//go:build ignore

// This build-tagged, non-default tool generates tests/boxo_0_42_2_records.txt
// with fresh keys. Copy it into a temporary Go module, require
// github.com/ipfs/boxo@v0.42.2, and redirect `go run main.go` to the fixture file.
// Boxo v0.42.2 corresponds to commit 25b1db8931508bb069eb6e67243b34d353cbe845.
// Each record passes Boxo ValidateWithName before the tool prints the fixture.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func emit(label string, keyType, bits int, value string, sequence uint64, ttl time.Duration, options ...ipns.Option) {
	privateKey, _, err := ic.GenerateKeyPairWithReader(keyType, bits, rand.Reader)
	if err != nil {
		panic(err)
	}
	contentPath, err := path.NewPath(value)
	if err != nil {
		panic(err)
	}
	record, err := ipns.NewRecord(
		privateKey,
		contentPath,
		sequence,
		time.Date(2100, time.January, 2, 3, 4, 5, 600_000_000, time.UTC),
		ttl,
		options...,
	)
	if err != nil {
		panic(err)
	}
	encoded, err := ipns.MarshalRecord(record)
	if err != nil {
		panic(err)
	}
	name, err := peer.IDFromPrivateKey(privateKey)
	if err != nil {
		panic(err)
	}
	if err := ipns.ValidateWithName(record, ipns.NameFromPeer(name)); err != nil {
		panic(err)
	}
	fmt.Printf("%s|%s|%s\n", label, name, hex.EncodeToString(encoded))
}

func main() {
	emit("v2-ed25519", ic.Ed25519, -1, "/ipfs/bafkqaaa", 7, 90*time.Second, ipns.WithV1Compatibility(false))
	emit("v2-rsa", ic.RSA, 2048, "/ipfs/bafkqaaa", 9, 2*time.Minute, ipns.WithV1Compatibility(false))
	emit("v1-v2-ed25519", ic.Ed25519, -1, "/ipfs/bafkqablimvwgy3y", 11, 3*time.Minute, ipns.WithV1Compatibility(true))
}
