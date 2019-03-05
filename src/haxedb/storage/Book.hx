package haxedb.storage;

import haxedb.record.books.BookRecord;
import haxedb.record.Record;
import sys.FileSystem;
import haxe.io.BytesInput;
import haxe.io.Bytes;
import sys.io.File;
import sys.io.FileSeek;
import haxedb.sys.System;

class Book {
	public var index(default, null):BookRecord;
	public var id(get, null):Int;
	public var pageSize(get, null):Int;
	public var dbFile(get, never):String;

	var persisted = false;

	function get_dbFile() {
		return './${this.index.blobFile}.db';
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
	}

	public static function fromIndex(index:BookRecord) {
		var book = new Book();
		book.index = index;
		book.init();
		return book;
	}

	public static function open(file = 'test') {
		var book = new Book();
		book.index.blobFile = file;
		book.init();
		return book;
	}

	function init() {
		var indexPage = readPage(0);
		if (indexPage == null) {
			writeIndexPage();
		} else {
			var indexData = indexPage.string();
			this.index = haxe.Unserializer.run(indexData);
		}
	}

	function writeIndexPage() {
		var indexPage = new Page(0, this);
		var serializer = new haxe.Serializer();
		serializer.serialize(index);
		indexPage.writeFromString(serializer.toString());
		this.persistPage(indexPage);
		persist(true);
	}

	function persist(isNew = false) {
		updateIndexPage();
		if (System.library != null) {
			if (isNew)
				this.index.id = System.library.count();
			var libRecord = System.library.getById(this.index.id);
			if (!this.persisted && libRecord == null) {
				System.log('Adding $dbFile to library! ${this.index}');
				System.library.addRecord(new Record<BookRecord>(this.index));
				this.persisted = true;
			} else {
				System.log('Updating $dbFile in library ${this.index}');
				if (libRecord.dbFile == this.dbFile)
					System.library.updateRecord(record -> record.data.id == this.index.id, this.index);
			}
			System.library.persistRecords();
		}
	}

	function updateIndexPage() {
		var indexPage = this.readPage(0);
		if (indexPage == null) {
			this.writeIndexPage();
			return;
		}
		var indexPage = new Page(0, this);
		var serializer = new haxe.Serializer();
		serializer.serialize(this.index);
		indexPage.writeFromString(serializer.toString());
		this.persistPage(indexPage);
	}

	// public function addPage(page:Page) {
	// 	this.pages.push(page);
	// }
	public function persistPage(page:Page) {
		var pageBytes = page.toBytes();
		var pid = page.id();
		var pageStart = pid * pageSize;
		var incrementNumPages = false;
		if (this.index.pages < pid) {
			incrementNumPages = true;
		}
		var bytes:Bytes = Bytes.alloc(pageSize);
		if (FileSystem.exists(this.dbFile)) {
			var input = File.read(this.dbFile, true);
			bytes = input.readAll();
		}
		if (pageStart + pageBytes.length >= bytes.length) {
			var length = (pageStart + pageBytes.length) - (bytes.length - 1);
			var newBytes = Bytes.alloc(bytes.length + length);
			newBytes.blit(0, bytes, 0, bytes.length);
			bytes = newBytes;
		}
		var pageSize = pageBytes.length;
		bytes.blit(pageStart, pageBytes, 0, pageSize);
		var output = File.write(this.dbFile, true);
		output.writeBytes(bytes, 0, bytes.length);
		output.flush();
		output.close();
		if (incrementNumPages) {
			this.index.pages++;
			this.persist();
		}
	}

	public function readPage(id:Int):Page {
		if (FileSystem.exists(this.dbFile)) {
			var input = File.read(this.dbFile, true);
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
