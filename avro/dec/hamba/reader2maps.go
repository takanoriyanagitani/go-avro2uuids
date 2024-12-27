package dec

import (
	"bufio"
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	as "github.com/takanoriyanagitani/go-avro2uuids"
	. "github.com/takanoriyanagitani/go-avro2uuids/util"
)

func ReaderToMapsHamba(
	rdr io.Reader,
	opts ...ho.DecoderFunc,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		var br io.Reader = bufio.NewReader(rdr)
		dec, e := ho.NewDecoder(br, opts...)
		if nil != e {
			yield(nil, e)
			return
		}

		var m map[string]any
		for dec.HasNext() {
			clear(m)
			e := dec.Decode(&m)
			if !yield(m, e) {
				return
			}
		}
	}
}

func ConfigToOptions(cfg as.InputConfig) []ho.DecoderFunc {
	var blobSizeMax int = cfg.BlobSizeMax
	var hcfg ha.Config
	hcfg.MaxByteSliceSize = blobSizeMax
	var hapi ha.API = hcfg.Freeze()
	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMaps(
	rdr io.Reader,
	cfg as.InputConfig,
) iter.Seq2[map[string]any, error] {
	var opts []ho.DecoderFunc = ConfigToOptions(cfg)
	return ReaderToMapsHamba(rdr, opts...)
}

func StdinToMaps(
	cfg as.InputConfig,
) iter.Seq2[map[string]any, error] {
	return ReaderToMaps(os.Stdin, cfg)
}

func ConfigToStdinToMaps(
	cfg as.InputConfig,
) IO[iter.Seq2[map[string]any, error]] {
	return func(_ context.Context) (iter.Seq2[map[string]any, error], error) {
		return StdinToMaps(cfg), nil
	}
}

var StdinToMapsDefault IO[iter.
	Seq2[map[string]any, error]] = ConfigToStdinToMaps(
	as.InputConfigDefault,
)
