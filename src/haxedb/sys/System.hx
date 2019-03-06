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
		log('--------------------------------------BOOT (${Date.now().toString()})------------------------------------------');
		if (!tryLoadFromFile()) {
			index = new SysIndex();
			var library = new Library(sysBook);
			var prefacePage = new Page(1, sysBook);
			index.library = library.index;
			prefacePage.writeFromString(haxe.Serializer.run(index));
			sysBook.persistPage(prefacePage);
			var collectionManager = new CollectionManager(sysBook);
			index.collectionManager = collectionManager.index;
			System.collectionManager.persist();
			// System.library.addRecord(new Record<BookRecord>(sysBook.index));
			// System.library.persist();

			var print = {id: prefacePage.id(), content: prefacePage.string(), nextPage: sysBook.nextFreePage()};
			log('preface: $print');
			log('Libraries: ${System.library}');
			log('Collections: ${System.collectionManager}');
		}
	}

	public static function log(text) {
		var fileContent = js.node.Fs.existsSync('./sys.db.log') ? js.node.Fs.readFileSync('./sys.db.log').toString() : "";
		js.node.Fs.writeFileSync('./sys.db.log', fileContent + '$text\n');
	}

	static function tryLoadFromFile():Bool {
		var prefacePage = sysBook.readPage(1);
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
		collectionManager.persist();
		library.persist();
		newIndex.library = library.index;
		newIndex.collectionManager = collectionManager.index;
		prefacePage.writeFromString(haxe.Serializer.run(newIndex));
		sysBook.persistPage(prefacePage);
		log('--------------------------------------END (${Date.now().toString()})------------------------------------------');
	}
}

class SysIndex {
	public function new() {}

	public var library:CollectionRecord;
	public var collectionManager:CollectionRecord;
}
