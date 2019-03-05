package haxedb.sys.collections;

import haxedb.storage.Collection;
import haxedb.record.collections.CollectionRecord;

class CollectionManager extends Collection<CollectionRecord> {
	public function getById<T>(id:Int) {
		var result = this.getRecord(record -> record.data.id == id);
		var collections = this.getRecords(record -> true);
		if (result == null)
			trace('$id not found in $collections');
		else
			trace('$id found in $collections: $result');
		return result != null ? Collection.load(result.data) : null;
	}

	public static function load(index:CollectionRecord):CollectionManager {
		var mgr = new CollectionManager(System.sysBook);
		mgr.setIndex(index);
		mgr.book = System.sysBook;
		trace('Loaded manager $mgr');
		return mgr;
	}
}
