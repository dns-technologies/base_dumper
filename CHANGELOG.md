# Version History

## 0.2.0.dev4

* Developer release (not public to pip)
* Update depends csvpack==0.1.0.dev4
* Improve get_query_kind() function
* Update pytests
* Update README.md

## 0.2.0.dev3

* Developer release (not public to pip)
* Change BaseDumper.dumper_mode attribute to method
* Fix BaseDumper._write_between() method
* Update DumperType
* Rename diagram.py	renders.py
* Rename transfer_diagram() to transfer_table()
* Rename table_diagram() to single_table()
* Rename log_diagram() to log_table()
* Update README.md

## 0.2.0.dev2

* Developer release (not public to pip)
* Add db_meta_from_iter() function
* Add BaseDumper._db_meta_from_iter() static method
* Add BaseDumper._db_meta_from_iter() static method
* Refactor BaseDumper._write_between() method
* Change dump_format atribute to methdod
* Revision DumperType
* Update pytests
* Update README.md

## 0.2.0.dev1

* Developer release (not public to pip)
* Add DBMetadata.close() method as patch
* Add DBMetadata.tell() method as patch
* Add DBMetadata.to_bytes() method as patch
* Add DBMetadata.to_rows() method as patch
* Add DBMetadata.to_pandas() method as patch
* Add DBMetadata.to_polars() method as patch
* Add depends csvpack==0.1.0.dev3
* Add CursorType, DumperType, ReaderType and WriterType types
* Add new methods
* Add chunk_bytes() function
* Add CSVStreamReader object
* Add BaseDumper.to_reader() metadata optional parameter
* Add source parameter into write methods
* Remove AbstractCursor class
* Remove ExampleReader class
* Remove BaseDumper.dbmeta attribute
* Refactor BaseDumper

## 0.2.0.dev0

* Developer release (not public to pip)
* Add depends csvpack==0.1.0.dev2
* Update depends light-compressor==0.1.1.dev1
* Change DumpFormat keys to `BINARY` and `CSV`
* Change BaseDumper.stream_type parameter to property method
* Add BaseDumper.s3_file boolean parameter. Default value is False
* Add BaseDumper.is_between boolean parameter. Default value is False
* Add csvpack imports
* Add pytest
* Add get_query_kind() function
* Add get_query_kind() and query_formatter() to imports
* Rename BaseDumper.`__version__` to BaseDumper.dumper_version
* Rename multiquery.py to query_parts.py
* Refactor multiquery() decorator
* Refactor BaseDumper.from_pandas() method
* Refactor EXECUTE_PATTERN (change quote to double quote)
* Remove pip.ini from root directory
* Update README.md

## 0.1.0.dev5

* Developer release (not public to pip)
* Improve DebugInfo object
* Improve transfer_table() function
* Improve multiquery decorator
* Add log_table() function
* Add single_table() function
* Add DumpFormat enum
* Add BaseDumper.dump_format parameter
* Remove BaseDumper.s3fs parameter
* Update README.md

## 0.1.0.dev4

* Developer release (not public to pip)
* Add DebugInfo object
* Improve BaseDumper.mode_action() method
* Improve chunk_query() function
* Improve multiquery() decorator
* Rename queryes -> queries
* Rename self.version -> self.`__version__`
* Add self.version as new parameter
* Refactor self.logger initialization
* Rename timeouts -> Timeout

## 0.1.0.dev4

* Developer release (not public to pip)
* Update depends light-compressor==0.1.0.dev4
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
