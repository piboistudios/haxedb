package haxedb.storage;

using Lambda;

import tink.io.Source;
import tink.io.Sink;
import haxedb.record.books.BookRecord;
import haxedb.record.Record;
import sys.FileSystem;
import haxe.io.BytesInput;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import tink.CoreApi;
import asys.io.File;
import sys.io.FileSeek;
import haxedb.sys.System;

class Book {
	public var index(default, null):BookRecord;
	public var id(get, null):Int;
	public var pageSize(get, null):Int;
	public var dbFile(get, never):String;

	var locked:Bool = false;
	var queue:Array<FutureTrigger<Noise>>;
	var persisted = false;

	static var openBooks:Map<String, Book> = new Map<String, Book>();

	function get_dbFile() {
		return './${this.index.blobFile}.db';
	}

	static var d = '---------';

	function lock(jumpQueue = false) {
		if (!this.locked) {
			System.log('${d}acquiring lock for  $dbFile$d');
			this.locked = true;
			this.locked = true;
			return Future.sync(Noise);
		} else {
			System.log('${d}queueing write to $dbFile$d');
			var trigger = Future.trigger();
			if (!jumpQueue)
				queue.push(trigger);
			else
				queue.insert(0, trigger);
			return trigger.asFuture();
		}
	}

	function unlock() {
		if (queue.length != 0) {
			System.log('${d}releasing lock for $dbFile (${queue.length})$d');

			var next = queue.splice(0, 1)[0];
			next.trigger(Noise);
		} else {
			System.log('${d}write queue emptied for $dbFile$d');
			this.locked = false;
		}
	}

	public function get_id() {
		return this.index.id;
	}

	function get_pageSize() {
		return this.index.pageSize;
	}

	public function nextFreePage() {
		return Std.int(Math.max(this.index.pages + 1, 1));
	}

	public function new() {
		this.index = new BookRecord();
		this.queue = [];
	}

	public static function fromIndex(index:BookRecord) {
		var book = new Book();
		book.index = index;
		book.init();
		return book;
	}

	public static function open(file = 'test') {
		return Future.async((cb:Book->Void) -> {
			System.log('--------------OPENING $file--------------');
			var book = new Book();
			book.index.blobFile = file;
			if (openBooks.exists(file)) {
				System.log('Pulling bookmark $book');
				cb(openBooks[file]);
				return Noise;
			} else {
				book.init().handle(() -> {
					openBooks.set(file, book);
					cb(book);
					System.log('Book opened $book');
				});
				return Noise;
			}
		});
	}

	function init() {
		return Future.async((done:Bool->Void) -> {
			var indexPage = readPage(0);
			if (indexPage == null) {
				writeIndexPage(true).handle((success:Bool) -> {
					System.log('Index written $index');
					done(success);
				});
			} else {
				var indexData = indexPage.string();
				if (indexData.length != 0) {
					this.index = haxe.Unserializer.run(indexData);
					System.log('Book loaded $index');
					this.persisted = true;
				}
				done(true);
			}
			return Noise;
		});
	}

	function writeIndexPage(isNew = false):Future<Bool> {
		if (isNew && System.library != null)
			this.index.id = System.library.count();
		return Future.async((done:Bool->Void) -> {
			var indexPage = new Page(0, this);
			var serializer = new haxe.Serializer();
			System.log('{  file: $dbFile\n  isNew: $isNew\n  Library: ${System.library}\n}');

			serializer.serialize(index);
			indexPage.writeFromString(serializer.toString());
			// System.log('index page: ${({id: indexPage.id(), content: indexPage.string(), expected: this.index})}');

			this.persistPage(indexPage, true).handle(() -> {
				// System.log("Persisting book");
				persist(isNew).handle(() -> {
					// System.log("Book persisted to library");
					done(true);
				});
			});
			this.unlock();
		});
	}

	function persist(isNew = false) {
		System.log('About to persist: $isNew $this');
		return Future.async((done:Bool->Void) -> {
			System.log('Persisting System.library: ${System.library}');
			if (System.library != null) {
				var libRecord = System.library.getById(this.index.id);
				var future = Future.sync(false);
				if (!this.persisted && libRecord == null) {
					System.log('Adding $dbFile to library! ${this.index}');
					// System.log('Adding $dbFile to library');
					future = System.library.addRecord(new Record<BookRecord>(this.index));
					this.persisted = true;
				} else {
					System.log('Updating $dbFile: ${this.index}\nExisting Record: $libRecord\n${System.library.getRecords(record -> true)}');
					// System.log('Updating $dbFile in library\nExisting Record: $libRecord');
					if (libRecord != null && libRecord.dbFile == this.dbFile)
						future = System.library.updateRecord(record -> record.data.id == this.index.id, this.index);
				}
				if (future == null)
					future = Future.sync(true);
				future.handle(() -> {
					System.log("Persisting library records");
					System.library.persistRecords().handle(() -> {
						// System.log('Done persisting SUCCESS $this\n${System.library}\n${System.library.getRecords(record -> true)}');
						done(true);
						return Noise;
					});
				});
			} else {
				done(true);
				// System.log('Done persisting book NO LIBRARY FOUND. $this');
			}
			return Noise;
		});
	} // public function addPage(page:Page) {

	// 	this.pages.push(page);
	// }
	public function persistPage(page:Page, jumpQueue = false):Future<Bool> {
		return Future.async((cb:Bool->Void) -> {
			var done = (bool) -> {
				unlock();
				cb(bool);
			}
			System.log('Preparing to write $page');
			lock(jumpQueue).handle(() -> {
				System.log('Lock acquired, beginning to write $page');
				var pageBytes = page.toBytes();
				var pid = page.id();
				var pageStart = pid * pageSize;
				var incrementNumPages = false;
				if (this.index.pages < pid) {
					// System.log('Saving ${({data: page, string: page.string()})}\nin Book: ${this.index}');
					incrementNumPages = true;
				}
				var _bytes:Bytes = Bytes.alloc(pageSize);
				var bOutput = new BytesOutput();

				var byteSink = Sink.ofOutput('Book.persistPage: empty byte array', bOutput);

				asys.FileSystem.exists(this.dbFile)
					.handle(exists -> {
						var doWrite = () -> {
							var bytes = bOutput.getBytes();
							// System.log('prewrite file-bytes: ${bytes.getString(0, bytes.length, haxe.io.Encoding.RawNative)}');
							if (pageStart + pageBytes.length >= bytes.length) {
								var length = (pageStart + pageBytes.length) - (bytes.length - 1);
								var newBytes = Bytes.alloc(bytes.length + length);
								newBytes.blit(0, bytes, 0, bytes.length);
								bytes = newBytes;
							}
							var pageSize = pageBytes.length;
							bytes.blit(pageStart, pageBytes, 0, pageSize);

							var inputStream = Source.ofInput('Book.persistPage: loaded byte array', new BytesInput(bytes));
							var outputStream = File.writeStream(this.dbFile, true);
							inputStream.pipeTo(outputStream)
								.handle(() -> {
									// outputStream.end()
									// System.log('\n------------------------------------------------------------------------------------------------------------------\n');
									// System.log('write file-bytes: ${bytes.getString(0, bytes.length, haxe.io.Encoding.RawNative)}');
									if (incrementNumPages) {
										this.index.pages++;
										// System.log("About to rewrite index");
										this.writeIndexPage()
											.handle(() -> {
												done(true);
											}); // System.log("Done rewriting index.");
									} else {
										// System.log("Done, index unchanged.");
										done(true);
									}
									return Noise;
									// System.log('Done writing..?\nPage: ${({writeStart: pageStart, id: page.id(), content: page.string()})}');
								});
						}
						if (exists) {
							var fileStream = File.readStream(this.dbFile, true);
							fileStream.pipeTo(byteSink)
								.handle(() -> doWrite());
						} else {
							doWrite();
						}
					});
			});
		});
	}

	public function readPage(id:Int):Page {
		if (sys.FileSystem.exists(this.dbFile)) {
			var input = sys.io.File.read(this.dbFile, true);
			input.seek(id * pageSize, FileSeek.SeekBegin);
			var page:Page = null;
			try {
				var id = input.readUInt24();
				var size = input.readUInt24();

				var content = input.read(size);
				input.close();
				page = new Page(id, this);
				page.writeFromBytes(content);
			} catch (eof:haxe.io.Eof) {
				return null;
			} catch (error:Dynamic) {
				throw error;
			}
			return page;
		} else
			return null;
	}
}
