fsdup
=====
`fsdup` is is a file system deduplication tool, mainly for deduping files _inside_ the NTFS filesystem. 
As of today, `fsdup` is a **work in progress** and barely tested, so please don't use it for anything important.
 
Usage
-----
Use `fsdup index` to index a disk to a store (file or Ceph backend) or filesystem and write out a manifest file. 
Once you have a manifest file, you can use it to map it to a drive and then mount it using `fsdup map`, 
or export the image using `fsdup export`.
  
```
Syntax:
  fsdup index [-debug] [-nowrite] [-store STORE] [-offset OFFSET] [-minsize MINSIZE] [-exact] INFILE MANIFEST
  fsdup map [-debug] MANIFEST
  fsdup export [-debug] MANIFEST OUTFILE
  fsdup print [-debug] MANIFEST
  fsdup stat [-debug] MANIFEST...
```