/////////////////////////////////////////
//mqtt2db - Node.js application: Store messages from Mosquitto MQTT broker into SQL Database
//@author : Ismet Said Calik
//@contact : ismetsaid.calik@gmail.com
//@website : http://calik.me
/////////////////////////////////////////

var mqtt = require('mqtt'); //https://www.npmjs.com/package/mqtt
var topicName = '#'; //subscribe to all topics

var brokerURL = 'mqtt://server.calik.me';
var databaseURL = 'server.calik.me';

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
	password: "root",
	database: "cartrack"
});

connection.connect(function(err) {
	if (err) throw err;
	console.log("Database Connected!");
});

function getDateTime() {
	var date = new Date();
	var hour = date.getHours();
	hour = (hour < 10 ? "0" : "") + hour;
	var min  = date.getMinutes();
	min = (min < 10 ? "0" : "") + min;
	var sec  = date.getSeconds();
	sec = (sec < 10 ? "0" : "") + sec;
	var year = date.getFullYear();
	var month = date.getMonth() + 1;
	month = (month < 10 ? "0" : "") + month;
	var day  = date.getDate();
	day = (day < 10 ? "0" : "") + day;
	return year + ":" + month + ":" + day + " " + hour + ":" + min + ":" + sec;
}



//insert a row into the devicelog table
function insert_message(topic, message_str, packet) {
	var message_arr = extract_string(message_str); //split a string into an array
	switch(topic)
	{
		case "/init":
		var deviceID= message_str;

		var date = getDateTime();
		var sql = "INSERT INTO ?? (??,??) VALUES (?,?)";
		var params = ['deviceinit', 'deviceID', 'date', deviceID, date];
		sql = mysql.format(sql, params);	

		connection.query(sql, function (error, results) {
			if (error) throw error;
			console.log("Message added to " + topic +" : "+ message_str);
		}); 
		break;

		case "/message":
		var deviceID= message_arr[0];
		var longitude = message_arr[1];
		var latitude = message_arr[2];
		var angle = message_arr[3];
		var date = getDateTime();
		var sql = "INSERT INTO ?? (??,??,??,??,??) VALUES (?,?,?,?,?)";
		var params = ['devicelogs', 'deviceID', 'date','longitude','latitude','angle', deviceID,date,longitude,latitude,angle];
		sql = mysql.format(sql, params);	

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