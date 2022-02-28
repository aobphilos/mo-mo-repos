
const  log = require("./utils/log");
const request = require("request");
const RabbitMQManager = require('../../messagelogic/src/components/RabbitMQManager').RabbitMQManager;
module.exports = function (RED) {
    function Chatlogic(n) {
        RED.nodes.createNode(this,n);
        var node = this;
        this.on("input",function(msg) {
            if (msg.payload) {
                const data = {
                    deviceType:msg.payload.deviceType || msg.deviceType,
                    deviceId:msg.payload.deviceId || msg.deviceId,
                    payload:msg.payload.payload || msg.payload,
                    eventType:"msg"
                };
                if(process.env.USE_MQ){
                    RabbitMQManager.getInstance().send(data);
                }
                else sendToChatlogic(data);
            } else {
                node.warn("payload is empty");
            }
        });
        function sendToChatlogic(data){
            const url = "http://"+(process.env.CHATLOGIC_HOST || "localhost")+":" + (process.env.CHATLOGIC_PORT || 1680) + "/chatlogic/receive/";
            log.debug("calling chatlogic at ", url);
            request({
                method: "POST",
                url: url,
                json: data,
            }, function (err, resp, json) {
                if (err || Math.floor(resp.statusCode / 100) != 2) {
                    log.error("error ", err, " and json ", json);
                }
                else {
                    log.debug("chatlogic response json ", json);
                }
            });
        }
    }
    RED.nodes.registerType("chatlogic",Chatlogic);
    //---------------------------------------- END OF NODE ---------------------------------------
}
