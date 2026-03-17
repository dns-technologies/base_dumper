# Version History

## 0.1.0.dev3

* Developer release (not public to pip)
* Update depends light-compressor==0.1.0.dev3
* Change compression_level to CompressionLevel.ZSTD_DEFAULT
* Add BaseDumper.mode_action() for DumperMode.DEBUG/TEST actions

## 0.1.0.dev2

* Developer release (not public to pip)
* Add s3fs: bool initialization param for upload dumps to s3 with s3fs library. Default is False.
* Update README.md

## 0.1.0.dev1

* Developer release (not public to pip)
* Fix comment in BaseDumper.__init__() section
* Add CompressionLevel import

## 0.1.0.dev0

* Developer release (not public to pip)
* Update depends pandas>=2.1.0
* Update depends polars>=0.20.31
* Update depends light-compressor==0.1.0.dev2
* Update README.md
* Add compression_level attribute

## 0.0.0.5

* Fix multiquery decorator
* Fix DumperLogger show version
* Fix chunk_query parts split
* Fix freeze to_reader() method where it have post queryes
* Change hidden methods to protected methods
* Rename AbstractReader to ExampleReader
* Update READEME.md

## 0.0.0.4

* Refactor BaseDumper
* Standalone decorator wrapper multiquery
* Unabstract property methods

## 0.0.0.3

* Fix logger_name

## 0.0.0.2

* Fix BaseDumper import

## 0.0.0.1

First version of base_dumper
