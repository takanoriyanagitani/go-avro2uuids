package avro2uuids

const BlobSizeMaxDefault int = 1048576

type InputConfig struct {
	BlobSizeMax int
}

var InputConfigDefault InputConfig = InputConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}
