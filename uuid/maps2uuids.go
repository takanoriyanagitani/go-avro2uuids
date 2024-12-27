package uuid

import (
	"context"
	"errors"
	"iter"

	gu "github.com/google/uuid"

	. "github.com/takanoriyanagitani/go-avro2uuids/util"
)

var (
	ErrIdMissing error = errors.New("uuid missing")
	ErrInvalidId error = errors.New("invalid uuid")
)

func MapsToUuids(
	m iter.Seq2[map[string]any, error],
	uuidColName string,
) IO[iter.Seq2[gu.UUID, error]] {
	return func(_ context.Context) (iter.Seq2[gu.UUID, error], error) {
		return func(yield func(gu.UUID, error) bool) {
			var id gu.UUID
			for row, e := range m {
				clear(id[:])

				if nil != e {
					yield(id, e)
					return
				}

				raw, found := row[uuidColName]
				if !found {
					yield(id, ErrIdMissing)
					return
				}

				switch t := raw.(type) {
				case [16]byte:
					copy(id[:], t[:])
				case []byte:
					if 16 != len(t) {
						yield(id, ErrInvalidId)
						return
					}
					copy(id[:], t[:])
				default:
					yield(id, ErrInvalidId)
					return
				}

				if !yield(id, nil) {
					return
				}
			}
		}, nil
	}
}

func UuidColNameToMapsToUuids(
	colname string,
) func(iter.Seq2[map[string]any, error]) IO[iter.Seq2[gu.UUID, error]] {
	return func(
		m iter.Seq2[map[string]any, error],
	) IO[iter.Seq2[gu.UUID, error]] {
		return MapsToUuids(m, colname)
	}
}
