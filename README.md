# haxedb
Applied database research

## What is it?
At the moment, this is simply applied research for possible future pursuits in developing a highly versatile cross-platform database.

The database supports concurrent reads/writes to separate books at once, and supports a locking queue for concurrent writes on the same book.

An example of a valid script:
```haxe
var run =  function(iterations) {
    db.collection('test').handle(test -> {
        var records = [];
        var startIndex = test.count();
        for(i in 0...iterations) {
            var myIndex = i + startIndex;
            records.push(db.record({name: 'test-'+(myIndex + 1), id: myIndex}));
        }
        return test.addRecords(records).handle(success -> {
            session.persist('success', success);
            session.persist('records', test.getRecords(r -> true));
            trace("Transaction 1 complete.");
        });
    });

}
session.persist('run', run);

```
Next REPL:
```
run(1000);
```


## How to run
The repo comes with the built Node.js (_test.js_) file, you should be able to run the file and create collections wherever the file is located.

It exposes a very simple API allowing one to access/manipulate collections. For more details see the Collection class in src/haxedb/storage/Collection.

![preview](https://i.ibb.co/MgK3Wb8/image.png)


