package haxedb.storage;

using Lambda;

import haxedb.storage.Page;
import haxedb.record.Record;

class RecordsPage<T> extends Page {
	public function new(id = -1, book:Book = null) {
		super(id, book);
	}

	public function records():Array<Record<T>> {
		var str = this.string();
		return str.length != 0 ? haxe.Unserializer.run(str) : [];
	}

	public function addRecord(record:Record<T>) {
		var records = this.records();
		record.location.pageNo = this.header.id;
		record.location.recordNo = records.length != 0 ? records[records.length - 1].location.recordNo + 1 : 0;
		records.push(record);
		return this.writeFromRecords(records);
	}

	public function addRecords(incomingRecords:Array<Record<T>>) {
		var records = this.records();
		var addRecord = (record:Record<T>) -> {
			record.location.pageNo = this.header.id;
			record.location.recordNo = records.length != 0 ? records[records.length - 1].location.recordNo + 1 : 0;
			records.push(record);
		};
        for(record in incomingRecords) {
            addRecord(record);
        }
        return this.writeFromRecords(records);
	}

	public function updateRecord(predicate:Record<T>->Bool, value:T) {
		var records = this.records();
		var recordToReplace = records.find(predicate);
		if (recordToReplace != null) {
			recordToReplace.data = value;
			return this.writeFromRecords(records);
		} else
			return false;
	}

	public function updateRecords(predicate:Record<T>->Bool, value:T) {
		var records = this.records();
		var recordsToReplace = records.filter(predicate);
		if (recordsToReplace != null && recordsToReplace.length != 0) {
			recordsToReplace.iter(record -> {
				record.data = value;
			});
			return this.writeFromRecords(records);
		} else
			return false;
	}

	public function getRecord(predicate:Record<T>->Bool) {
		var records = this.records();
		return records.find(predicate);
	}

	public function getRecords(predicate:Record<T>->Bool) {
		var records = this.records();
		return records.filter(predicate);
	}

	public function removeRecord(recordNo:Int) {
		var records = this.records();
		var recordToRemove = records.find(record -> record.location.recordNo == recordNo);
		if (recordToRemove == null)
			return false;
		records.remove(recordToRemove);
		return this.writeFromRecords(records);
	}

	function writeFromRecords(records:Array<Record<T>>) {
		this.book.persistPage(this);
		return this.writeFromString(haxe.Serializer.run(records));
	}

	public static function fromPage<T>(page:Page):RecordsPage<T> {
		var recordsPage = new RecordsPage<T>(page.id(), page.book);
		recordsPage.writeFromString(page.string());
		return recordsPage;
	}
}
