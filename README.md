fsdup
=====
`fsdup` is a file system deduplication tool, mainly for deduping files _inside_ the NTFS filesystem. 
As of today, `fsdup` is a **work in progress** and barely tested, so please don't use it for anything important.
 
Usage
-----
Use `fsdup index` to index/dedup a disk or filesystem and store the resulting chunks in Ceph or on disk. After the 
index process, you'll have a manifest file describing the indexed disk/filesystem which can then be used
to map it to a local drive using the `fsdup map` command, or export the image using `fsdup export`.

Example
-------
```
$ fsdup index /dev/loop1 mydisk.manifest
  # Writes chunks to 'index' folder (default) and generates mydisk.manifest

$ fsdup index -store /mnt/myindex ...
  # Writes chunks to '/mnt/myindex' folder instead
  
$ fsdup index -store 'ceph:ceph.conf?pool=chunkindex' ...
  # Writes chunks to Ceph pool 'chunkindex' instead, using 
  # the 'ceph.conf' config file

$ fsdup print mydisk.manifest
idx 0000000000 diskoff 0000000000000 - 0000000196608 len 196608        gap        chunk a4bfc70231b80a636dfc2aeff92bfbc18c3225a424be097e88173d5fce1f68ae chunkoff          0 -     196608
idx 0000000001 diskoff 0000000196608 - 0000039288832 len 39092224      sparse     -
idx 0000000002 diskoff 0000039288832 - 0000039759872 len 471040        gap        chunk b8976c52c266cca71fbc16a4195b7a28cf7b2088d862e2590c246e3a9620864f chunkoff          0 -     471040
...

$ fsdup export mydisk.manifest mydisk.img
  # Creates an image file from chunk index using the manifest
  
$ fsdup map mydisk.manifest
  # Creates a block device /dev/nbdX that allows reading the image file without exporting it

$ fsdup stat *.manifest
Manifests:               79
Number of unique chunks: 6210530
Total image size:        22.0 TB (24167171188224 bytes)
Total on disk size:      9.3 TB (10181703710208 bytes)
Total sparse size:       12.7 TB (13985467478016 bytes)
Total chunk size:        6.2 TB (6805815365169 bytes)
Average chunk size:      1.0 MB (1095851 bytes)
Dedup ratio:             1.5 : 1
Space reduction ratio:   33.2 %   
```

Commands
--------
```
Syntax:
  fsdup index [-debug] [-nowrite] [-store STORE] [-offset OFFSET] [-minsize MINSIZE] [-exact] INFILE MANIFEST
  fsdup map [-debug] MANIFEST
  fsdup export [-debug] MANIFEST OUTFILE
  fsdup print [-debug] MANIFEST
  fsdup stat [-debug] MANIFEST...
```

License
-------
[Apache License 2.0](LICENSE)

Author
------
Philipp Heckel