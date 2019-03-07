package haxedb.record.collections;

import haxedb.sys.System;

class CollectionRecord {
	public var pages:Array<Int>;
	public var bookId:Int;
	public var id:Int = 0;

	public function new() {
		if (System.collectionsInitialized && System.collectionManager != null)
			this.id = System.collectionManager.count();
	}
}