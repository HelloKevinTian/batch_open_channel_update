var redis = require("redis")
var fs = require('fs');
var async = require('async');

var client = redis.createClient("25040", "10.10.2.183", {
	"no_ready_check": true
});
// var client = redis.createClient("6379", "127.0.0.1", {
// 	"no_ready_check": false
// });

var config = JSON.parse(fs.readFileSync('config.json', "utf8"));

// console.log(config,typeof config);

var redis_table = 'h_notice';


client.on("error", function(err) {
	console.log("Error " + err);
});

client.on("connect", run);


function run() {
	async.waterfall([
		function(callback) {
			client.hkeys(redis_table, function(err, reply) {
				if (err) throw err;

				if (reply != null) {
					callback(null, reply);
				}
			});
		},
		function(keys, callback) {
			client.hvals(redis_table, function(err, reply) {
				if (err) throw err;

				if (reply != null) {
					callback(null, keys, reply);
				}
			});
		}
	], function(err, keys, vals) {
		async.each(config, function(item, callback) {
			process(item, keys, vals);
		}, function(err) {
			console.log("all over...   ", err);
		});
	});


	// hgetall();
}

function hgetall() {
	client.hgetall(redis_table, function(err, reply) {
		if (err) throw err;

		if (reply != null) {
			console.log(reply, typeof reply);
		}
	});
	// client.hkeys(redis_table, function(err, reply) {
	// 	if (err) throw err;

	// 	if (reply != null) {
	// 		console.log(reply, typeof reply);
	// 	}
	// });
	// 	client.hvals(redis_table, function(err, reply) {
	// 		if (err) throw err;

	// 		if (reply != null) {
	// 			console.log(reply, typeof reply);
	// 		}
	// 	});
}

function process(item, keys, vals) {
	// console.log(item, "---", keys, "----", vals);

	var list = item.versions;
	var count = 0;

	async.whilst(
		function() {
			return count < list.length;
		},
		function(callback) {
			var key = item.channel + ":" + list[count];
			var index = keys.indexOf(key);

			if (index !== -1) {
				var result = new Object();
				result = JSON.parse(vals[index]);
				result.update = item.update;
				result.force_update = item.force_update;
				result.update_url = item.update_url;
				client.hset(redis_table, key, JSON.stringify(result), function(err) {
					if (err) throw err;
					console.log("update ok...  ",key);

					count++;
					callback();
				});
			} else {
				var key1 = "template:" + list[count];
				client.hget(redis_table, key1, function(err, tp_data) {
					if (err) throw err;

					if (tp_data != null) {
						var result = new Object();
						result = JSON.parse(tp_data);
						result.update = item.update;
						result.force_update = item.force_update;
						result.update_url = item.update_url;
						client.hset(redis_table, key, JSON.stringify(result), function(err) {
							if (err) throw err;
							console.log("insert ok...  ",key);

							count++;
							callback();
						});
					}
				});
			}
		},
		function(err) {
			console.log("all over ...");
		}
	);
}