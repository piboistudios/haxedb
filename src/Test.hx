package;

import haxedb.storage.Book;
import haxedb.storage.Page;

class Test {
	static var book = new Book('test-db-pages');

	public static function main() {
		// for (i in 0...10000) {
		// 	writePage(i);
		// }
		// for (i in 25...300) {
		// 	modifyPage(i);
		// }
		// for (i in 0...1000) {
		// 	readPage(i);
		// }
		// modifyPage(0);
		// readPage(0);
		// div();
		// trace("Done");
		writePage(-1);
		trace(book);
	}

	static function div() {
		trace('------------------------------------------------------------------');
	}

	static function writePage(pageNo:Int) {
		var page = new Page(pageNo, book);
		var pageWriteSuccess = page.writeFromString("{'data':'data'}");
		// trace('Page write successful? $pageWriteSuccess');
		var pageBytes = page.toBytes();
		var pageFromBytes = Page.fromBytes(pageBytes);
		var serialize = haxe.Json.stringify;

		// trace({before: serialize(page), after: serialize(pageFromBytes)});
		book.persistPage(page);
	}

	static function readPage(pageNo:Int) {
		var pageFromDisk = book.readPage(pageNo);
		trace('Page From Disk:');
		trace(pageFromDisk.string());
	}

	static function modifyPage(pageNo:Int) {
		var page = book.readPage(pageNo);
		page.writeFromString("{'data': 'NEW DATA'}");
		book.persistPage(page);
	}
}
