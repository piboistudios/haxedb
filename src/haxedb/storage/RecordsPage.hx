package haxedb.storage;

using Lambda;

import tink.CoreApi;
import haxedb.storage.Page;
import haxedb.record.Record;

typedef PreprocessResult<T> = {
	var spaceAvailable:Bool;
	var records:Array<Record<T>>;
}

class RecordsPage<T> extends Page {
	public function new(id = -1, book:Book = null) {
		super(id, book);
	}

	public function records():Array<Record<T>> {
		var str = this.string();
		return str.length != 0 ? haxe.Unserializer.run(str) : [];
	}

	public function addRecord(record:Record<T>):PreprocessResult<T> {
		var records = this.records();
		record.location.pageNo = this.header.id;
		record.location.recordNo = records.length != 0 ? records[records.length - 1].location.recordNo + 1 : 0;
		records.push(record);
		return {spaceAvailable: this.hasSpaceFor(records), records: records};
	}

	// public function addRecord(record:Record<T>):Promise<Bool> {
	// 	retuspaceAvailable: rn {this.hasSpaceFor(records)records: , records};
	// }
	public function addRecords(incomingRecords:Array<Record<T>>):PreprocessResult<T> {
		var records = this.records();
		var addRecord = (record:Record<T>) -> {
			record.location.pageNo = this.header.id;
			record.location.recordNo = records.length != 0 ? records[records.length - 1].location.recordNo + 1 : 0;
			records.push(record);
		};
		for (record in incomingRecords) {
			addRecord(record);
		}
		return {spaceAvailable: this.hasSpaceFor(records), records: records};
	}

	public function updateRecord(predicate:Record<T>->Bool, value:T):PreprocessResult<T> {
		var records = this.records();
		var recordToReplace = records.find(predicate);
		var recordExists = false;
		if (recordToReplace != null) {
			recordToReplace.data = value;
			recordExists = true;
		}
		return {
			spaceAvailable:this.hasSpaceFor(records) && recordExists, records:records
		};
	}

	public function updateRecords(predicate:Record<T>->Bool, value:T):PreprocessResult<T> {
		var records = this.records();
		var recordsToReplace = records.filter(predicate);
		var recordChanged = false;
		if (recordsToReplace != null && recordsToReplace.length != 0) {
			recordsToReplace.iter(record -> {
				record.data = value;
			});
			recordChanged = true;
		}
		return {
			spaceAvailable:this.hasSpaceFor(records) && recordChanged, records:records
		};
	}

	public function getRecord(predicate:Record<T>->Bool) {
		var records = this.records();
		return records.find(predicate);
	}

	public function getRecords(predicate:Record<T>->Bool) {
		var records = this.records();
		return records.filter(predicate);
	}

	public function removeRecord(recordNo:Int):PreprocessResult<T> {
		var records = this.records();
		var recordToRemove = records.find(record -> record.location.recordNo == recordNo);
		if (recordToRemove == null)
			return {spaceAvailable: false, records: records};
		records.remove(recordToRemove);
		return {spaceAvailable: this.hasSpaceFor(records), records: records};
	}

	public function commit(records:Array<Record<T>>):Promise<Bool> {
		var successfulWrite = this.writeFromString(haxe.Serializer.run(records));
		return Future.async((cb:Bool->Void) -> {
			if (successfulWrite)
				this.book.persistPage(this).handle(() -> {
					cb(successfulWrite);
				});
			else
				Future.sync(successfulWrite);
			return Noise;
		});
	}

	function hasSpaceFor(records:Array<Record<T>>):Bool {
		return canFit(haxe.Serializer.run(records));
	}

	public static function fromPage<T>(page:Page):RecordsPage<T> {
		var recordsPage = new RecordsPage<T>(page.id(), page.book);
		recordsPage.writeFromString(page.string());
		return recordsPage;
	}
}
