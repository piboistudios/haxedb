package haxedb.storage;

import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.io.BytesInput;
import haxe.io.Encoding;

class Page {
	var header:Header;
	var contents:Bytes;

	public var book(default, null):Book = null;

	public function new(id = -1, book:Book = null) {
		this.book = book;
		if (id == -1 && book != null) {
			id = book.nextFreePage();
		}
		this.header = {id: id, size: 0};
		this.contents = Bytes.ofString('');
	}

	public function id() {
		return this.header.id;
	}

	public function size() {
		return this.header.size;
	}

	public function updateSize() {
		this.header.size = this.toBytes(false).length;
	}

	public function writeFromHex(hex:String):Bool {
		var bytes = Bytes.ofHex(hex);
		return writeFromBytes(bytes);
	}

	public function writeFromString(string:String):Bool {
		var bytes = Bytes.ofString(string);
		return this.writeFromBytes(bytes);
	}

	public function writeFromBytes(data:Bytes):Bool {
		if (data.length < pageSize()) {
			this.contents = data;

			this.updateSize();
			return true;
		}
		return false;
	}

	public function string():String {
		return this.contents.toString();
	}

	public function hex():String {
		return this.contents.toHex();
	}

	public function raw() {
		return this.contents;
	}

	public function padded() {
		var padLength = pageSize() - this.header.size;
		var bytesOutput = new BytesOutput();
		bytesOutput.write(this.contents);
		for (i in 0...padLength) {
			bytesOutput.writeByte(0);
		}
		var bytes = bytesOutput.getBytes();
		bytesOutput.close();
		return bytes;
	}

	public function toBytes(padding = true) {
		var output = new BytesOutput();
		output.writeUInt24(this.header.id);
		output.writeUInt24(this.header.size);
		output.write(padding ? this.padded() : this.contents);
		var bytes = output.getBytes();
		output.close();
		return bytes;
	}

	public static function fromBytes(bytes:Bytes):Page {
		var input = new BytesInput(bytes);
		var id = input.readUInt24();
		var size = input.readUInt24();
		var page = new Page(id);
		page.writeFromBytes(input.read(size - input.position));
		input.close();
		return page;
	}

	function pageSize() {
		return book != null ? book.pageSize : 8000;
	}
}

typedef Header = {
	var id:Int;
	var size:Int;
}
