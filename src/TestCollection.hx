package;

using Lambda;

import haxedb.sys.System;
import haxedb.storage.Book;
import haxedb.storage.Collection;
import haxedb.record.Record;

typedef DObj = {
	var name:String;
	var age:Int;
	var job:String;
}

class TestCollection {
	public static function main() {
		init();
		for (i in 0...50) {
			insertRecord();
		}
		readAll();
		close();
	}

	static var book:Book;
	static var collection:Collection<DObj>;

	static function close() {
		trace('CLOSING -------------------------------');
		trace(System.collectionManager.getRecords(record -> true).map(record -> haxe.Json.stringify(record)));
		trace(System.library.getRecords(record -> true).map(record -> haxe.Json.stringify(record)));
		System.teardown();
	}

	static function init() {
		System.init();
        trace('OPENING -------------------------------');
		trace(System.collectionManager.getRecords(record -> true).map(record -> haxe.Json.stringify(record)));
		trace(System.library.getRecords(record -> true).map(record -> haxe.Json.stringify(record)));
		book = Book.open('test-collection-2020');
		var loadedCollection = System.collectionManager.getRecord(record -> record.data.bookId == book.index.id);
		collection = loadedCollection != null ? Collection.load(loadedCollection.data) : new Collection<DObj>(book);
		if (loadedCollection != null) {
			trace('---------------------------LOAD SUCCESS');
		}
	}

	static var names = [
		'jim', 'bob', 'ralph', 'joe', 'steve', 'john', 'dave', 'paul', 'kevin', 'larry', 'ed'
	];
	static var jobs = ['work', 'lead', 'follow', 'independent', 'contract', 'unemployed'];

	static function insertRecord() {
		var data = {
			name: names[Std.int((Math.random() * 100) % names.length)],
			age: Std.int(Math.random() * 100),
			job: jobs[Std.int((Math.random() * 100) % jobs.length)]
		};
		var record = new Record(data);
		collection.addRecord(record);
		collection.persist();
	}

	static function readAll() {
		js.node.Fs.writeFileSync('whole-db.json', haxe.Json.stringify(collection.getRecords(record -> true)));
	}
}
