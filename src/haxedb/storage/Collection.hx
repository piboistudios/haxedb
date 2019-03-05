package haxedb.storage;

using Lambda;

import haxedb.sys.System;
import haxedb.record.collections.CollectionRecord;
import haxedb.record.Record;

class Collection<T> {
	public var index(default, null):CollectionRecord;

	var pages:Map<Int, RecordsPage<T>>;
	var book:Book;
	var dirtyPages:Array<Int>;

	public function new(book:Book) {
		this.index = new CollectionRecord();
		this.index.pages = [];
		if (book != null) {
			this.index.bookId = book.id;
			this.book = book;
		}
		this.pages = [];
		this.dirtyPages = [];
	}

	public static function load<T>(index:CollectionRecord) {
		if (System.library != null) {
			trace('My book id is ${index.bookId}');
			var book = System.library.getById(index.bookId);
			if (book == null)
				throw 'Cannot resolve collection-book ${index.bookId}';
			var newCollection = new Collection<T>(book);
			newCollection.setIndex(index);
			return newCollection;
		} else
			throw "Cannot load collection-books without library.";
	}

	public function persist() {
		if (System.collectionManager != null) {
			trace('TRYING TO PERSIST $index');
			if (System.collectionManager.getById(this.index.id) == null) {
				System.collectionManager.addRecord(new Record<CollectionRecord>(this.index));
				System.collectionManager.persistRecords();
			}
		}
	}

	public function persistRecords() {
		if (this.book == null)
			throw "Cannot persist page with no book.";
		this.dirtyPages.iter(pageNo -> {
			var page = this.getPage(pageNo);
			this.book.persistPage(page);
			trace("Persisted dirty page");
			trace({
				id: page.id(),
				content: page.string()
			});
			trace('In book: $book');
		});
		this.dirtyPages = [];
	}

	public function setIndex(index:CollectionRecord) {
		this.index = index;
	}

	public function getBook() {
		return System.library != null ? System.library.getById(this.index.bookId) : System.sysBook;
	}

	function getPage(pageNo:Int):RecordsPage<T> {
		if (this.pages.exists(pageNo))
			return this.pages[pageNo];
		else {
			if (this.book != null) {
				this.pages.set(pageNo, RecordsPage.fromPage(this.book.readPage(pageNo)));
				return this.pages[pageNo];
			}
		}
		return null;
	}

	public function addRecord(record:Record<T>) {
		if (this.book != null)
			trace('$record will be added to ${this.book.dbFile}');
		for (pageNo in this.index.pages) {
			var page = this.getPage(pageNo);
			trace('Trying page: ${page.string()}');
			if (page != null && page.addRecord(record)) {
				this.dirtyPages.push(page.id());
				return true;
			} else {
				var pageInfo = {id: page.id(), content: page.string(), size: page.size()};
				trace('Could not add $record to $pageInfo');
			}
		}
		trace('$record couldn\'t fit existing pages ${this.index.pages}');
		try {
			var newPage = new RecordsPage<T>(-1, this.book);

			this.index.pages.push(newPage.id());
			this.pages.set(newPage.id(), newPage);
			if (this.book != null) {
				this.book.persistPage(newPage);
				this.dirtyPages.push(newPage.id());
				trace('adding record to new page: $newPage');
				return newPage.addRecord(record);
			} else
				return newPage.addRecord(record);
		} catch (ex:Dynamic) {
			throw ex;
		}
	}

	public function getRecord(predicate:Record<T>->Bool) {
		for (pageNo in this.index.pages) {
			var page = this.getPage(pageNo);
			var result = page != null ? page.getRecord(predicate) : null;
			if (result != null)
				return result;
		}
		return null;
	}

	public function count() {
		return this.getRecords(record -> true).length;
	}

	public function getRecords(predicate:Record<T>->Bool) {
		var aggregate = [];
		trace('Getting records from pages: ${this.index.pages}');
		for (pageNo in this.index.pages) {
			var page = this.getPage(pageNo);
			var result = page != null ? page.getRecords(predicate) : null;
			if (result != null && result.length != 0)
				aggregate = aggregate.concat(result);
		}
		return aggregate;
	}

	public function removeRecord(record:Record<T>) {
		for (pageNo in this.index.pages) {
			var page = this.getPage(pageNo);
			if (page != null && page.removeRecord(record.location.recordNo)) {
				this.dirtyPages.push(page.id());
				return true;
			}
		}
		return false;
	}
}
