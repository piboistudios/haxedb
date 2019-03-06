package haxedb.sys.books;

import haxedb.storage.Collection;
import haxedb.storage.Book;
import haxedb.record.books.BookRecord;
import haxedb.sys.System;
import haxedb.record.collections.CollectionRecord;
import haxedb.record.Record;

class Library extends Collection<BookRecord> {
	public function getById(id:Int):Book {
		var result = this.getRecord(record -> record.data.id == id);
		return result != null ? Book.fromIndex(result.data) : null;
	}

	public static function load(index:CollectionRecord):Library {
		var lib = new Library(System.sysBook);
		lib.loaded = true;
		lib.setIndex(index);
		lib.book = System.sysBook;
		return lib;
	}

	public override function addRecord(record:Record<BookRecord>) {
		var records = this.getRecords(record -> true);
		
		return super.addRecord(record);
	}

	public override function persistRecords(force = false) {
		var allRecords = this.getRecords(record -> true);
		super.persistRecords(force);
		var allNewRecords = this.getRecords(record -> true);
		
	}
}
