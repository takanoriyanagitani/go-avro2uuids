package raw

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	gu "github.com/google/uuid"

	. "github.com/takanoriyanagitani/go-avro2uuids/util"
)

type RawUuidWriter func(iter.Seq2[gu.UUID, error]) IO[Void]

func RawUuidWriterNew(w io.Writer) RawUuidWriter {
	return func(ids iter.Seq2[gu.UUID, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			var bw *bufio.Writer = bufio.NewWriter(w)
			defer bw.Flush()

			for id, e := range ids {
				select {
				case <-ctx.Done():
					return Empty, ctx.Err()
				default:
				}

				if nil != e {
					return Empty, e
				}

				_, e = bw.Write(id[:])
				if nil != e {
					return Empty, e
				}
			}

			return Empty, nil
		}
	}
}

func RawUuidWriterNewStdout() RawUuidWriter {
	return RawUuidWriterNew(os.Stdout)
}
