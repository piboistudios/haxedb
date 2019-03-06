package;

import haxedb.storage.Book;
import haxedb.storage.Collection;
import haxedb.sys.System;
import haxedb.record.Record;

typedef DObj = {
	var name:String;
	var age:Int;
	var job:String;
}

class TestCollections {
	static var book:Book;
	static var collection:Collection<DObj>;

	public static function main() {
		init();
		genCollection();
	}

	static function init() {
		System.init();
		book = Book.open('test-collection');
		collection = new Collection<DObj>(book);
		trace("Created book:");
		trace(book);
		trace("Created collection:");
		trace(collection);
	}

	static function genCollection() {
		for (i in 0...10000) {
			var data = {name: 'bob', age: 22, job: 'work'};
			var record = new Record<DObj>(data);
			collection.addRecord(record);
		}
		collection.commitAll();
	}
}
