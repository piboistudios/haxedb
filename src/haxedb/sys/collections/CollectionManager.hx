package haxedb.sys.collections;

import haxedb.storage.Collection;
import haxedb.record.collections.CollectionRecord;
import haxedb.storage.Book;

class CollectionManager extends Collection<CollectionRecord> {
	public function getById<T>(id:Int) {
		var result = this.getRecord(record -> record.data.id == id);
		return result != null ? Collection.load(result.data) : null;
	}
	
	public static function load(index:CollectionRecord):CollectionManager {
		var mgr = new CollectionManager(System.sysBook);
		mgr.loaded = true;
		mgr.setIndex(index);
		mgr.book = System.sysBook;
		return mgr;
	}
}
