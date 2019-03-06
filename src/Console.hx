package;

using StringTools;
using Lambda;

import hscript.Parser;
import hscript.Interp;
import haxedb.sys.System;
import haxedb.storage.Book;
import haxedb.storage.Collection;
import haxedb.record.Record;

class Console {
	public static function main() {
		init();
		run();
	}

	static function init() {
		System.init();
		var dbApi:haxe.DynamicAccess<Dynamic> = new haxe.DynamicAccess<Dynamic>();
		for (key in api.keys()) {
			dbApi.set(key, api[key]);
		}
		trace('api: $dbApi');
		interp.variables.set('db', dbApi);
		printInstructions();
	}

	static function printInstructions() {
		trace("API:");
		trace("_________");
		for (key in apiDefinitions.keys()) {
			trace('$key - ${apiDefinitions[key]}');
		}
	}

	static function teardown() {
		System.teardown();
	}

	static var rl:js.node.readline.Interface;
	static var interp = new Interp();
	static var parser = new Parser();
	static var scriptLines = [];
	static var indenters = ['{', '(', '['];
	static var dedenters = ['}', ')', ']'];

	static function run() {
		if (rl == null)
			rl = js.node.Readline.createInterface(js.Node.process.stdin, js.Node.process.stdout);

		rl.question(getIndent(), _input -> {
			var input = _input.trim();
			if (input != '.end' && input != '') {
				if (input == '.exit') {
					teardown();
					rl.close();
				
					// return;
				} 
				else if(input == '.abort') {
					scriptLines = [];
					run();
				}
				else {
					scriptLines.push(input);
					run();
				}
			} else {
				var script = scriptLines.join('\n');
				try {
					trace('script: $script');
					var ast = parser.parseString(script);
					trace(interp.execute(ast));
				} catch (error:Dynamic) {
					trace('ERROR: $error');
				}
				scriptLines = [];
				run();
			}
		});
	}

	static var indents = 1;

	static function getIndent() {
		var retVal = ">>";
		for (i in 0...indents) {
			retVal += "  ";
		}
		return retVal;
	}

	static var api:Map<String, Dynamic> = [
		'collection' => (collectionName:String) -> {
			var book = Book.open(collectionName);
			var collection = Collection.fromBook(book);
			return collection;
		},
		'record' => (data:Dynamic) -> {
			return new Record(data);
		},
		'persist' => interp.variables.set
	];
	static var apiDefinitions:Map<String, String> = [
		'collection' => '(collectionName:String) -> Collection - Retrieves a collection by name; creates one if one doesn\'t exist',
		'record' => '(data:Dynamic) -> Record -  Creates a new record object from some data'
	];
}
