package haxedb.record;

class Record<T> {
	public var data:T;
	public var location:BlobLoc;

	public function new(data:T) {
		this.data = data;
		this.location = {pageNo: 0, recordNo: 0};
	}
}

typedef BlobLoc = {
	var pageNo:Int;
	var recordNo:Int;
}
