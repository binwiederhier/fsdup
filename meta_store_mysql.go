package fsdup

import (
	"database/sql"
	"encoding/hex"
	"fmt"
)

type mysqlMetaStore struct {
	db *sql.DB
}

var _ MetaStore = &mysqlMetaStore{}

func NewMysqlMetaStore(dataSource string) (*mysqlMetaStore, error) {
	debugf("Creating MySQL metadata store using datasource %s\n", dataSource)

	db, err := sql.Open("mysql", dataSource) // "fsdup:fsdup@/fsdup"
	if err != nil {
		return nil, err
	}

	return &mysqlMetaStore{
		db: db,
	}, nil
}

func (s *mysqlMetaStore) ReadManifest(manifestId string) (*manifest, error) {
	manifest := NewManifest(DefaultChunkSizeMaxBytes) // FIXME
	rows, err := s.db.Query(
		"SELECT checksum, chunkOffset, chunkLength, kind FROM manifest WHERE manifestId = ? ORDER BY offset ASC",
		manifestId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	diskfrom := int64(0)
	for rows.Next() {
		var chunkOffset, chunkLength int64
		var kindStr string
		var checksum sql.NullString

		if err := rows.Scan(&checksum, &chunkOffset, &chunkLength, &kindStr); err != nil {
			return nil, err
		}

		kind, err := kindFromString(kindStr)
		if err != nil {
			return nil, err
		}

		var checksumBytes []byte
		if checksum.Valid {
			checksumBytes, err = hex.DecodeString(checksum.String)
			if err != nil {
				return nil, err
			}
		}

		manifest.Add(&chunkSlice{
			checksum:  checksumBytes, // Can be nil!
			kind:      kind,
			diskfrom:  diskfrom,
			diskto:    diskfrom + chunkLength,
			chunkfrom: chunkOffset,
			chunkto:   chunkOffset + chunkLength,
			length:    chunkLength,
		})

		diskfrom += chunkLength
	}

	return manifest, nil
}

func (s* mysqlMetaStore) WriteManifest(manifestId string, manifest *manifest) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO manifest 
		SET 
			manifestId = ?,
			offset = ?,
			checksum = ?,
			chunkOffset = ?,
			chunkLength = ?,
			kind = ?
`)
	if err != nil {
		return err
	}
	defer stmt.Close() // danger!

	for _, offset := range manifest.Offsets() {
		slice := manifest.Get(offset)
		if slice.kind == kindSparse {
			_, err = stmt.Exec(manifestId, offset, &sql.NullString{},
				slice.chunkfrom, slice.length, slice.kind.toString())
		} else {
			_, err = stmt.Exec(manifestId, offset, fmt.Sprintf("%x", slice.checksum),
				slice.chunkfrom, slice.length, slice.kind.toString())
		}
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
