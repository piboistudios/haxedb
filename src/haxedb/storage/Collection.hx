package haxedb.storage;

using Lambda;

import tink.CoreApi;
import haxedb.sys.System;
import haxedb.record.collections.CollectionRecord;
import haxedb.record.Record;
import haxedb.storage.RecordsPage;

class Collection<T> {
	public var index(default, null):CollectionRecord;
	public var pages(default, null):Map<Int, RecordsPage<T>>;

	var book:Book;
	var dirtyPages:Array<Int>;
	var loaded = false;

	public function new(book:Book, loaded = false) {
		this.index = new CollectionRecord();
		this.loaded = loaded;
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
			var library = System.library;
			System.log('Library: $library\n${library.getRecords(record -> true)}');
			var book = System.library.getById(index.bookId);
			if (book == null)
				throw 'Cannot resolve collection-book ${index.bookId}';
			var newCollection = new Collection<T>(book, true);
			newCollection.setIndex(index);
			return newCollection;
		} else
			throw "Cannot load collection-books without library.";
	}

	public function persist():Promise<Bool> {
		return Future.async((done:Bool->Void) -> {
			this.persistRecords().handle(_ -> {
				if (System.collectionManager != null) {
					if (!this.loaded && this.index.id == 0) {
						this.index.id = System.collectionManager.count();
					}
					var promise = if (System.collectionManager.getById(this.index.id) == null) {
						System.collectionManager.addRecord(new Record<CollectionRecord>(this.index));
					} else {
						System.collectionManager.updateRecord(record -> record.data.id == this.index.id, this.index);
					}
					promise.next(success -> {
						System.collectionManager.persistRecords(true).handle(_ -> {
							done(true);
						});
					});
				}
			});
		});
	}

	inline function isCollectionMgr() {
		return this.book == System.sysBook;
	}

	public function persistRecords(force = false) {
		if (this.book == null)
			throw "Cannot persist page with no book.";

		var pages = (!force ? this.dirtyPages : this.index.pages);
		var futures:Array<Future<Bool>> = [];
		pages.iter(pageNo -> {
			var page = this.getPage(pageNo);
			var pageData = {
				id: page.id(),
				content: page.string()
			};

			futures.push(this.book.persistPage(page));
		});
		this.dirtyPages = [];
		return Future.ofMany(futures);
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

	public function addRecords(records:Array<Record<T>>) {
		return Future.async((done:Bool->Void) -> {
			for (pageNo in this.index.pages) {
				var page = this.getPage(pageNo);
				var result = page.addRecords(records);
				if (page != null && result.spaceAvailable) {
					page.commit(result.records).next((success:Bool) -> {
						if (success) {
							this.dirtyPages.push(page.id());
							this.persist();
							done(true);
						}
						return Noise;
					});
					return Noise;
				}
			}
			try {
				var newPage = new RecordsPage<T>(-1, this.book);
				if (this.index.pages.find(page -> page == newPage.id()) != null) {
					newPage.setId(newPage.id() + 1);
				}
				this.index.pages.push(newPage.id());
				this.pages.set(newPage.id(), newPage);
				if (this.book != null) {
					var retVal = false;
					var result = newPage.addRecords(records);
					// System.log("Adding records to new page 3960");

					// System.log("Beginning new page");
					retVal = result.spaceAvailable;
					if (!result.spaceAvailable) {
						// System.log("Bisecting");
						var bisection = Std.int(records.length / 2);
						var records1 = records.slice(0, bisection);
						var records2 = records.slice(bisection);
						var futures = [this.addRecords(records1), this.addRecords(records2)];
						Future.ofMany(futures).handle(successes -> {
							done(successes[0] && successes[1]);
						});
					} else {
						newPage.commit(result.records)
							.next((proceed:Bool) -> {
								if (proceed) {
									this.dirtyPages.push(newPage.id());
									this.book.persistPage(newPage)
										.handle(succcess -> {
											this.persist()
												.handle(success -> {
													done(retVal);
												});
										});
								}
								return Noise;
							});
					}
					return Noise;
				} else {
					this.persist().next(_ -> {
						return newPage.addRecords(records);
					}).next((success:PreprocessResult<T>) -> {
						done(success.spaceAvailable);
						return Noise;
					});
					return Noise;
				}
			} catch (ex:Dynamic) {
				throw ex;
			}
		});
	}

	public function addRecord(record:Record<T>) {
		return Future.async((cb:Bool->Void) -> {
			for (pageNo in this.index.pages) {
				var page = this.getPage(pageNo);
				var result = page.addRecord(record);
				if (page != null && result.spaceAvailable) {
					page.commit(result.records).next(success -> {
						if (success) {
							this.dirtyPages.push(page.id());
							this.persist();
							cb(true);
						}
						return Noise;
					});
					return Noise;
				}
			}

			try {
				var newPage = new RecordsPage<T>(-1, this.book);

				this.index.pages.push(newPage.id());
				this.pages.set(newPage.id(), newPage);

				if (this.book != null) {
					var retVal = false;
					var result = newPage.addRecord(record);
					newPage.commit(result.records)
						.next(result -> {
							retVal = result;
							return this.book.persistPage(newPage);
						})
						.next(_ -> {
							this.dirtyPages.push(newPage.id());
							return this.persist();
						})
						.next(_ -> {
							cb(retVal);
							return Noise;
						});
					return Noise;
				} else {
					this.persist();
					var result = newPage.addRecord(record);
					newPage.commit(result.records).next((success:Bool) -> {
						cb(success);
						return Noise;
					});
					return Noise;
				}
			} catch (ex:Dynamic) {
				throw ex;
			}
		});
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

	public function updateRecord(predicate:Record<T>->Bool, value:T) {
		return Future.async((cb:Bool->Void) -> {
			for (pageNo in this.index.pages) {
				var page = this.getPage(pageNo);
				var result = page.updateRecord(predicate, value);
				if (page != null && result.spaceAvailable) {
					page.commit(result.records).next((success:Bool) -> {
						if (success) {
							this.dirtyPages.push(page.id());
							cb(true);
						}
						return Noise;
					});
					return;
				}
			}
			cb(false);
		});
	}

	public function updateRecords(predicate:Record<T>->Bool, value:T) {
		var retVal = false;
		return Future.async((cb:Bool->Void) -> {
			for (pageNo in this.index.pages) {
				var page = this.getPage(pageNo);
				var result = page.updateRecords(predicate, value);
				if (page != null && result.spaceAvailable) {
					page.commit(result.records).next((success:Bool) -> {
						if (success) {
							this.dirtyPages.push(page.id());
							retVal = true;
						}
						return Noise;
					});

					return;
				}
			}
			cb(retVal);
		});
	}

	public function getRecords(predicate:Record<T>->Bool) {
		var aggregate = [];
		for (pageNo in this.index.pages) {
			var page = this.getPage(pageNo);
			var result = page != null ? page.getRecords(predicate) : null;
			if (result != null && result.length != 0)
				aggregate = aggregate.concat(result);
		}
		return aggregate;
	}

	public function removeRecord(record:Record<T>) {
		return Future.async((done:Bool->Void) -> {
			for (pageNo in this.index.pages) {
				var page = this.getPage(pageNo);
				var result = page.removeRecord(record.location.recordNo);
				if (page != null && result.spaceAvailable) {
					page.commit(result.records).next((success:Bool) -> {
						if (success) {
							this.dirtyPages.push(page.id());
							done(true);
						}
						return Noise;
					});
					return;
				}
			}
			done(false);
		});
	}

	public static function fromBook<T>(book:Book) {
		var loadedCollection = System.collectionManager.getRecord(record -> record.data.bookId == book.index.id);
		if (loadedCollection != null) {
			return Collection.load(loadedCollection.data);
		} else {
			return new Collection<T>(book);
		}
	}
}
