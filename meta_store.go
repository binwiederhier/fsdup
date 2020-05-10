package fsdup

type MetaStore interface {
	ReadManifest(manifestId string) (*manifest, error)
	WriteManifest(manifestId string, manifest *manifest) error
}
