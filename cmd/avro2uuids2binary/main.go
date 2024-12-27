package main

import (
	"context"
	"fmt"
	"iter"
	"log"
	"os"

	gu "github.com/google/uuid"

	. "github.com/takanoriyanagitani/go-avro2uuids/util"

	u "github.com/takanoriyanagitani/go-avro2uuids/uuid"
	wr "github.com/takanoriyanagitani/go-avro2uuids/uuid/writer/raw"

	dh "github.com/takanoriyanagitani/go-avro2uuids/avro/dec/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var uuidColumnName IO[string] = EnvValByKey("ENV_UUID_COLNAME")

var uuids IO[iter.Seq2[gu.UUID, error]] = Bind(
	uuidColumnName,
	func(colname string) IO[iter.Seq2[gu.UUID, error]] {
		return Bind(
			stdin2maps,
			u.UuidColNameToMapsToUuids(colname),
		)
	},
)

var stdin2maps2uuids2raw2stdout IO[Void] = Bind(
	uuids,
	wr.RawUuidWriterNewStdout(),
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2maps2uuids2raw2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
