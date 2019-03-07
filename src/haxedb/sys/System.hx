package haxedb.sys;

import tink.CoreApi;
import haxedb.storage.Page;
import haxedb.storage.Book;
import haxedb.sys.books.Library;
import haxedb.sys.collections.CollectionManager;
import haxedb.record.Record;
import haxedb.record.books.BookRecord;
import haxedb.record.collections.CollectionRecord;

class System {
	public static var sysBook:Book;
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
		trace("DB Init");
		return Future.async((done:Bool->Void) -> {
			trace("Open System Book");
			Book.open('sys').handle((system:Book) -> {
				sysBook = system;
				trace('Got system book? $sysBook');
				if (!tryLoadFromFile()) {
					index = new SysIndex();
					var library = new Library(sysBook);
					var collectionManager = new CollectionManager(sysBook);
					var prefacePage = new Page(1, sysBook);
					index.library = library.index;
					index.collectionManager = collectionManager.index;
					prefacePage.writeFromString(haxe.Serializer.run(index));
					trace("Persisting preface?");
					sysBook.persistPage(prefacePage);
					done(true);
				} else {
					done(true);
				}
			});
		});
	}

	public static function log(text) {
		var fileContent = js.node.Fs.existsSync('./sys.db.log') ? js.node.Fs.readFileSync('./sys.db.log').toString() : "";
		js.node.Fs.writeFileSync('./sys.db.log', fileContent + '$text\n');
	}

	static function tryLoadFromFile():Bool {
		var prefacePage = sysBook.readPage(1);
		// trace('preface: ${({id: prefacePage.id(), content: prefacePage.string()})}');
		if (prefacePage != null) {
			index = haxe.Unserializer.run(prefacePage.string());
			_library = Library.load(index.library);
			_collectionManager = CollectionManager.load(index.collectionManager);
			System.log('Loaded\n$_library\n${_library.getRecords(record -> true)}\n$_collectionManager\n${_collectionManager.getRecords(record -> true)}');
			return true;
		} else {
			return false;
		}
	}

	static public function teardown() {
		var prefacePage = new Page(1, sysBook);
		var newIndex = new SysIndex();

		collectionManager.persist()
			.handle(() -> {
				library.persist()
					.handle(() -> {
						newIndex.library = library.index;
						newIndex.collectionManager = collectionManager.index;
						trace("Shutting down.");
						System.log('Close Status: \n$_library\n${_library.getRecords(record -> true)}\n$_collectionManager\n${_collectionManager.getRecords(record -> true)}');
						prefacePage.writeFromString(haxe.Serializer.run(newIndex));
						sysBook.persistPage(prefacePage)
							.handle(() -> {
								System.log('Preface: ${newIndex}');
								log('--------------------------------------END (${Date.now().toString()})------------------------------------------');
							});
					});
			});
	}
}

class SysIndex {
	public function new() {}

	public var library:CollectionRecord;
	public var collectionManager:CollectionRecord;
}
