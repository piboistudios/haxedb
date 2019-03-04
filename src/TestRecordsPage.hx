package;

import haxedb.storage.*;
import haxedb.record.Record;

typedef DObj = {
	var name:String;
	var age:Int;
	var job:String;
}

class TestRecordsPage {
	static var book = new Book('test-db-record-pages');

	public static function main() {
		insertRecords(1);
		insertRecords(1);
		insertRecords(1);
		insertRecords(1);
		readPage(1);
	}

	static function writePage() {
		var page = new RecordsPage<DObj>(-1, book);
		var data = {name: 'bob', age: 22, job: 'work'};
		var record1 = new Record<DObj>(data);
		var record2 = new Record<DObj>(data);
		var record3 = new Record<DObj>(data);
		page.addRecord(record1);
		page.addRecord(record2);
		page.addRecord(record3);
		book.persistPage(page);
	}

	static function insertRecords(pageNo:Int) {
		var page:RecordsPage<DObj> = RecordsPage.fromPage(book.readPage(pageNo));
        var data = {name: 'bob', age: 22, job: 'work'};
		var record1 = new Record<DObj>(data);
		var record2 = new Record<DObj>(data);
		var record3 = new Record<DObj>(data);
		page.addRecord(record1);
		page.addRecord(record2);
		page.addRecord(record3);
		book.persistPage(page);
	}

	static function readPage(pageNo:Int) {
		var page:RecordsPage<DObj> = RecordsPage.fromPage(book.readPage(pageNo));
		trace(page.string());
		trace(page.records());
	}
}
