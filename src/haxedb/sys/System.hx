package haxedb.sys;

import haxedb.storage.Page;
import haxedb.storage.Book;
import haxedb.sys.books.Library;
import haxedb.sys.collections.CollectionManager;
import haxedb.record.Record;
import haxedb.record.books.BookRecord;
import haxedb.record.collections.CollectionRecord;

class System {
	public static var sysBook = Book.open('sys');
	public static var library(get, null):Library;
	public static var collectionManager(get, null):CollectionManager;
	public static var collectionsInitialized = false;
	static var _library:Library;
	static var _collectionManager:CollectionManager;
	static var index:SysIndex;

	public static function get_library() {
		if (_library == null && index != null && index.library != null) {
			_library = Library.load(index.library);
		}
		return _library;
	}

	public static function get_collectionManager() {
		if (_collectionManager == null && index != null && index.collectionManager != null) {
			_collectionManager = CollectionManager.load(index.collectionManager);
			collectionsInitialized = true;
		}
		return _collectionManager;
	}

	public static function init() {
		if (!tryLoadFromFile()) {
			index = new SysIndex();
			var library = new Library(sysBook);
			var collectionManager = new CollectionManager(sysBook);
			var prefacePage = new Page(1, sysBook);
			index.library = library.index;
			index.collectionManager = collectionManager.index;
			prefacePage.writeFromString(haxe.Serializer.run(index));
			sysBook.persistPage(prefacePage);
			System.library.addRecord(new Record<BookRecord>(sysBook.index));
			var print = {id: prefacePage.id(), content: prefacePage.string(), nextPage: sysBook.nextFreePage()};
			trace('preface: $print');
		} else {
			trace("successfully loaded from file");
			trace(index);
		}
	}

	static function tryLoadFromFile():Bool {
		var prefacePage = sysBook.readPage(1);
		trace("Preface:");
		if (prefacePage != null)
			trace(prefacePage.string());
		else
			trace('null');
		if (prefacePage != null) {
			index = haxe.Unserializer.run(prefacePage.string());
			_library = Library.load(index.library);
			_collectionManager = CollectionManager.load(index.collectionManager);
			return true;
		} else {
			return false;
		}
	}

	static public function teardown() {
		var prefacePage = new Page(1, sysBook);
		var newIndex = new SysIndex();
		newIndex.library = library.index;
		newIndex.collectionManager = collectionManager.index;
		library.persistRecords();
		collectionManager.persistRecords();
		collectionManager.persist();
		library.persist();
		prefacePage.writeFromString(haxe.Serializer.run(newIndex));
		sysBook.persistPage(prefacePage);
	}
}

class SysIndex {
	public function new() {}

	public var library:CollectionRecord;
	public var collectionManager:CollectionRecord;
}
