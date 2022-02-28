/**
 * Created by touchaponk on 28/12/2015.
 */

var request = require("request");
//var Log = require('log');
//var log = new Log(process.env.log);
var log = require("./log");
require('sugar');
var chrono = require('chrono-node');
var sherlock = require("../vendor/sherlock");
var
    checkAndOr = function (sentence, andOrString) {
        var andArr = andOrString.split(",");
        for (var i = 0; i < andArr.length; i++) {
            var orArr = andArr[i].split("/");
            var found = false;
            for (var j = 0; j < orArr.length; j++) {
                var w = orArr[j].trim();
                var english = /^[a-zA-Z \.!?_\s@\-\)\(]*$/;
                if (english.test(w)) {
                    var regexp = new RegExp("\\b" + w + "\\b", "i");
                }
                else var regexp = new RegExp(w);
                //var engTest = /^[A-Za-z0-9_,.-@$()?!\s]*/;
                //var regexp = new RegExp('' + w + ''); // USING THIS UNTIL THAI LANG IDENTIFICATION IS IN PLACE
                log.debug("[Util] Evaling -", w, "- against " + sentence, " result ", regexp.test(sentence));
                //if (a.indexOf(w) != -1)break;
                if (regexp.test(sentence))found = true;
            }
            log.debug("[Util] end evaling ", andArr[i], " as ", orArr, " result ", found);
            if (!found) {
                return false;
            }
        }
        return true;
    },
    parseAndOr = function (sentence, andOrString) {
        var andArr = andOrString.split(",");
        for (var i = 0; i < andArr.length; i++) {
            var orArr = andArr[i].split("/");
            var found = false;
            for (var j = 0; j < orArr.length; j++) {
                var w = orArr[j].trim();
                //var english = /^[A-Za-z0-9]*$/;
                //if(english.test(w))w = " "+w+" ";
                var regexp = new RegExp('\\b' + w + '\\b');
                log.debug("Evaling -", w, "- against " + input, " result ", regexp.test(input));
                //if (a.indexOf(w) != -1)break;
                if (regexp.test(input))found = true;
            }
            log.debug("end evaling ", andArr[i], " as ", orArr, " result ", found);
            if (!found) {
                return false;
            }
        }
        return true;
    },
    parseVariable = function (definition, variable) {
        return definition.split(".").reduce(function (currentVar, needle) {
            //console.log("getting ", needle, "from", currentVar)
            return (needle && currentVar && currentVar[needle]) ? currentVar[needle] : null;
        }, variable);

    },
    parseDate = function (sentence) {
        sentence = sentence.replace(/\./, ":");
        var sugar_date = Date.future(sentence).utc(true);
        log.debug("[Util] sugarjs parse date result for ", sentence, " is ", sugar_date);
        if (sugar_date != "Invalid Date") return sugar_date;
        var chrono_result = chrono.parse(sentence);
        log.debug("[Util] chrono parse date result for ", sentence, " is ", chrono_result);
        if (chrono_result.length == 0) {
            //chronojs failed
            var sherlock_res = sherlock.parse(sentence).startDate;
            log.debug("[Util] sherlock parse date result for ", sentence, " is ", sherlock_res);
            return sherlock_res;
        }
        else {
            return chrono_result[0].start.date();
        }

    },
    parseTime = function (sentence) {
        sentence = sentence.replace(/\./, ":");
        var sugar_date = Date.future(sentence).utc(true);
        var nowRef = new Date();
        var sugarRef = new Date();
        sugarRef.setHours(sugar_date.getHours());
        sugarRef.setMinutes(sugar_date.getMinutes());
        sugarRef.setSeconds(sugar_date.getSeconds());
        sugarRef.setMilliseconds(sugar_date.getMilliseconds());
        var msDiff = Math.abs(sugarRef.getTime() - nowRef.getTime());
        log.debug("[Util] sugarjs parse time result for ", sentence, " is ", sugar_date, " ms diff is ", msDiff);
        if (sugar_date != "Invalid Date"  && (msDiff >= 3)) return sugar_date;
        var chrono_result = chrono.parse(sentence);
        log.debug("[Util] chrono parse time result for ", sentence, " is ", chrono_result);
        if (chrono_result.length == 0) {
            //chronojs failed
            var sherlock_res = sherlock.parse(sentence).startDate;
            log.debug("[Util] sherlock parse time result for ", sentence, " is ", sherlock_res);
            return sherlock_res;
        }
        else {
            var known = Object.keys(chrono_result[0].start.knownValues);
            var chrono_time_known = known.indexOf("hour") >= 0;
            if(!chrono_time_known){
                var sherlock_res = sherlock.parse(sentence).startDate;
                log.debug("[Util] sherlock parse time result for ", sentence, " is ", sherlock_res);
                return sherlock_res;
            }
            else return chrono_result[0].start.date();
        }
    },
    parseDateTime = function(sentence){
        var date = parseDate(sentence);
        if(!date)return null;
        var time = parseTime(sentence);
        if(!time)return null;
        log.debug("[Util] parse datetime result date:", date," time ", time);
        date.setHours(time.getHours());
        date.setMinutes(time.getMinutes());
        date.setSeconds(time.getSeconds());
        date.setMilliseconds(time.getMilliseconds());
        log.debug("[Util] final time is :", date);
        return date;
    };
module.exports = {
    checkAndOr: checkAndOr,
    parseAndOr: parseAndOr,
    parseVariable: parseVariable,
    parseDate: parseDate,
    parseTime: parseTime,
    parseDateTime : parseDateTime
};