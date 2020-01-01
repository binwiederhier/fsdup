package fsdup

type MetaStore interface {
	GetManifest(manifestId string) (*manifest, error)
	PutManifest(manifestId string) (int, error)
}
