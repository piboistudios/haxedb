package haxedb.storage;

class Collection<T> {
	var pages:Array<Int>;
	var bookId:Int;

	public function new(book:Book) {
		this.pages = new Array<Int>();
		this.bookId = bookId;
	}

	public function getBook() {
		return BookMap.getById(bookId);
	}

	public function addRecord(record:Record<T>) {
		for (page in pages) {
			if (page.addRecord(record))
				return true;
		}
		try {
			var newPage = new RecordsPage<T>(-1, book);
			newPage.addRecord(record);
			this.pages.push(newPage.id());
			return true;
		} catch (ex:Dynamic) {
			throw ex
		}
	}

	public function removeRecord(record:Record<T>) {
		for (page in pages) {
			if (page.removeRecord(record)) {
				return true;
			}
		}
		return false;
	}
}
