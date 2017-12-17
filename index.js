/////////////////////////////////////////
//mqtt2db - Node.js application: Store messages from Mosquitto MQTT broker into SQL Database
//@author : Ismet Said Calik
//@contact : ismetsaid.calik@gmail.com
//@website : http://calik.me
/////////////////////////////////////////

var mqtt = require('mqtt'); //https://www.npmjs.com/package/mqtt
var topicName = '#'; //subscribe to all topics

var brokerURL = 'mqtt://vtsp.cf';
var databaseURL = 'vtsp.cf';

var options = {
	clientId: 'mqtt2db',
	port: 1883,
	//username: 'zxijxflo',
	//password: '8def8ac3',	
	keepalive : 60
};

var client  = mqtt.connect(brokerURL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);

function mqtt_connect() {
	console.log("Connecting MQTT");
	client.subscribe(topicName, mqtt_subscribe);
};

function mqtt_subscribe(err, granted) {
	console.log("Subscribed to " + topicName);
	if (err) {console.log(err);}
};

function mqtt_reconnect(err) {
	console.log("Reconnect MQTT");
	if (err) {console.log(err);}
	client  = mqtt.connect(brokerURL, options);
};

function mqtt_error(err) {
	console.log("Error!");
	if (err) {console.log(err);}
};

function after_publish() {
	//do nothing
};

//receive a message from MQTT broker
function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString(); //convert byte array to string
	message_str = message_str.replace(/\n$/, ''); //remove new line
	insert_message(topic, message_str, packet);
};

function mqtt_close() {
	console.log("Close MQTT");
};

//mySQL
var mysql = require('mysql'); //https://www.npmjs.com/package/mysql
//Create Connection
var connection = mysql.createConnection({
	host: databaseURL,
	user: "root",
	password: "BvfynUzuGXrN5uwe",
	database: "vts"
});

connection.connect(function(err) {
	if (err) throw err;
	console.log("Database Connected!");
});

//insert a row into the devicelog table
function insert_message(topic, message_str, packet) {
	var message_arr = extract_string(message_str); //split a string into an array
	switch(topic)
	{
		//353115085687217@28603
		case "/init":
		var deviceID= message_arr[0];
		var operator= message_arr[1];
		var sql = "INSERT INTO ?? (??,??) VALUES (?,?)";
		var params = ['deviceinit', 'deviceID', 'operator', deviceID,operator];
		sql = mysql.format(sql, params);	
		connection.query(sql, function (error, results) {
			if (error) throw error;
			console.log("Message added to " + topic +" : "+ message_str);
		});
		
		//console.log(message_str[0] +" - "+ message_str[1]); 
		break;

		//353115085687217@38.670876@39.219166@1130.20@0.00@0.00
		case "/message":
		var deviceID= message_arr[0];
		var longitude = message_arr[1];
		var latitude = message_arr[2];
		var altitude = message_arr[3];
		var speed = message_arr[4];
		var angle = message_arr[5];

		var sql = "INSERT INTO ?? (??,??,??,??,??,??) VALUES (?,?,?,?,?,?)";
		var params = ['devicelogs', 'deviceID','longitude','latitude','altitude','angle','speed', deviceID,longitude,latitude,altitude,angle,speed];
		sql = mysql.format(sql, params);	
		//console.log(sql);
		connection.query(sql, function (error, results) {
			if (error) throw error;
			console.log("Message added to " + topic +" : "+ message_str);
		}); 
		break;
	}
};	

//split a string into an array of substrings
function extract_string(message_str) {
	var message_arr = message_str.split("@"); //convert to array	
	return message_arr;
};