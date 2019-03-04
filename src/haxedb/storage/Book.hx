package haxedb.storage;

import sys.FileSystem;
import haxe.io.BytesInput;
import haxe.io.Bytes;
import sys.io.File;
import sys.io.FileSeek;

class Book {
	var index:Index;
	var pages:Array<Int>;
	var blobFile:String;

	public var pageSize:Int = 8000;

	var dbFile(get, never):String;

	function get_dbFile() {
		return './${this.blobFile}.db';
	}

	public function nextFreePage() {
		return this.index.pages + 1;
	}

	public function new(file = 'test') {
		this.blobFile = file;
		this.init();
	}

	function init() {
		var indexPage = readPage(0);
		if (indexPage == null) {
			writeIndexPage();
		} else {
			var indexData = indexPage.string();
			trace(indexPage);
			trace('indexData: $indexData');
			this.index = haxe.Unserializer.run(indexData);
			trace(this.index);
		}
	}

	function writeIndexPage() {
		var newIndex:Index = new Index();
		var indexPage = new Page(0, this);
		var serializer = new haxe.Serializer();
		serializer.serialize(newIndex);
		indexPage.writeFromString(serializer.toString());
		this.index = newIndex;
		this.persistPage(indexPage);
	}

	function updateIndexPage() {
		var indexPage = this.readPage(0);
		if (indexPage == null)
			throw "Cannot update null index page.";
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
		trace('${pageStart}, ${pid}, ${pageSize}:  ${pageBytes.length}/${bytes.length}');
		var pageSize = pageBytes.length;
		bytes.blit(pageStart, pageBytes, 0, pageSize);
		var output = File.write(this.dbFile, true);
		output.writeBytes(bytes, 0, bytes.length);
		output.flush();
		output.close();
		if (incrementNumPages) {
			this.index.pages++;
			if (pid != 0)
				this.updateIndexPage();
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
			}
			return page;
		} else
			return null;
	}
}

class Index {
	public var pages:Int = -1;
	public var id:Int = 0;
	public function new() {

	}
}
