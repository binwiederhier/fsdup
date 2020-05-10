package fsdup

type fileMetaStore struct {
	// Nothing
}

func NewFileMetaStore() *fileMetaStore {
	return &fileMetaStore{}
}

func (s *fileMetaStore) GetManifest(manifestId string) (*manifest, error) {
	return NewManifestFromFile(manifestId)
}

func (s* fileMetaStore) PutManifest(manifestId string, manifest *manifest) error {
	return manifest.WriteToFile(manifestId)
}