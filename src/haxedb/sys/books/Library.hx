package haxedb.sys.books;

import haxedb.storage.Collection;
import haxedb.storage.Book;
import haxedb.record.books.BookRecord;
import haxedb.sys.System;
import haxedb.record.collections.CollectionRecord;

class Library extends Collection<BookRecord> {
	public function getById(id:Int):Book {
		var result = this.getRecord(record -> record.data.id == id);
		var allRecords = this.getRecords(record -> true);
		trace(allRecords);
		trace("All records above");
		return result != null ? Book.fromIndex(result.data) : null;
	}

	public static function load(index:CollectionRecord):Library {
		var lib = new Library(System.sysBook);
		lib.setIndex(index);
		lib.book = System.sysBook;
		return lib;
	}

	public override function persistRecords() {
		trace('Persisting library records. ${this.index.pages} to ${this.book}');
		super.persistRecords();
	}
}
