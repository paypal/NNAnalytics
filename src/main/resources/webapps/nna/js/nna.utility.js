/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
function nFormatter(num, digits) {
    var si = [
        { value: 1E18, symbol: "E" },
        { value: 1E15, symbol: "P" },
        { value: 1E12, symbol: "T" },
        { value: 1E9,  symbol: "G" },
        { value: 1E6,  symbol: "M" },
        { value: 1E3,  symbol: "k" }
    ], rx = /\.0+$|(\.[0-9]*[1-9])0+$/, i;
    for (i = 0; i < si.length; i++) {
        if (num >= si[i].value) {
        return (num / si[i].value).toFixed(digits).replace(rx, "$1") + si[i].symbol;
        }
    }
    return num.toFixed(digits).replace(rx, "$1");
}

function getPercentage(num1,num2) {
    var a = Number(num1);
    var b = Number(num2);
    var c = (a/b)*100;

    return c.toFixed(2);
}

function removeParam(key, sourceURL) {
    var rtn = sourceURL.split("?")[0],
    param,
    params_arr = [],
    queryString = (sourceURL.indexOf("?") !== -1) ? sourceURL.split("?")[1] : "";
    if (queryString !== "") {
        params_arr = queryString.split("&");
        for (var i = params_arr.length - 1; i >= 0; i -= 1) {
            param = params_arr[i].split("=")[0];
            if (param === key) {
                params_arr.splice(i, 1);
            }
        }
        rtn = rtn + "?" + params_arr.join("&");
    }
    return rtn;
}

function addParameterToURL(key, value, sourceURL){
    key = encodeURI(key);
    value = encodeURI(value);

    var kvp = sourceURL.substr(0).split('&');
    var i=kvp.length; var x; while(i--) {
        x = kvp[i].split('=');

        if (x[0]==key) {
            x[1] = value;
            kvp[i] = x.join('=');
            break;
        }
    }

    if(i<0) {
        kvp[kvp.length] = [key,value].join('=');
    }
    return kvp.join('&');
}

function determineNsColumnName() {
    var name = getUrlParameter("sum");
    if (name == "quotaAssigned") {
        return "Namespace Quota Assigned";
    } else if (name == "quotaUsed") {
        return "Namespace Quota Used";
    } else {
        return "Namespace Quota % Used";
    }
}

function determineDsColumnName() {
    var name = getUrlParameter("sum");
    if (name == "quotaAssigned") {
        return "Diskspace Quota Assigned";
    } else if (name == "quotaUsed") {
        return "Diskspace Quota Used";
    } else {
        return "Diskspace Quota % Used";
    }
}

function getParameter(name, sourceUrl) {
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
    var results = regex.exec(sourceUrl);
    return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
}

function getUrlParameter(name) {
    name = name.replace(/[\[]/, '\\[').replace(/[\]]/, '\\]');
    var regex = new RegExp('[\\?&]' + name + '=([^&#]*)');
    var results = regex.exec(location.search);
    return results === null ? '' : decodeURIComponent(results[1].replace(/\+/g, ' '));
}

function msToTime(duration) {
    var milliseconds = parseInt((duration%1000)/100)
        , seconds = parseInt((duration/1000)%60)
        , minutes = parseInt((duration/(1000*60))%60)
        , hours = parseInt((duration/(1000*60*60))%24);

    hours = (hours < 10) ? "0" + hours : hours;
    minutes = (minutes < 10) ? "0" + minutes : minutes;
    seconds = (seconds < 10) ? "0" + seconds : seconds;

    return hours + ":" + minutes + ":" + seconds + "." + milliseconds;
}

function msToDate(report) {
    var d = new Date(report);
    return d.toLocaleString();
}

function replaceWithBreaks(text) {
    return text.replace(/(?:\r\n|\r|\n)/g, '<br />');
}

function printObject(o) {
    return JSON.stringify(JSON.decycle(o), replacer);
}

function replacer(key, value) {
    if (key=="backgroundColor") {
        return "";
    }
    if (key=="label") {
        return "";
    }
    if (key=="_meta") {
        return "";
    }
    return value;
}

function getRandomColor() {
    var letters = '0123456789ABCDEF'.split('');
    var color = '#';
    for (var i = 0; i < 6; i++ ) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}

function getLinkColor(value,name){
    switch(name){
        case "emptyFile":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "emptyDir":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "tinyFile":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "smallFile":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "mediumFile":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "oldFile":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
        case "quotas":
            if(value > 10.0){
               return 'list-group-item list-group-item-danger';
            }
            if(value > 0.0){
               return 'list-group-item list-group-item-warning';
            }
            return 'list-group-item list-group-item-success';
    }
}

function getButtonColor(value,name){
    switch(name){
        case "emptyFile":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "emptyDir":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "tinyFile":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "smallFile":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "mediumFile":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "oldFile":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
        case "quotas":
            if(value > 10.0){
               return 'btn btn-danger btn-xs';
            }
            if(value > 0.0){
               return 'btn btn-warning btn-xs';
            }
            return 'btn btn-success btn-xs';
   }
}

function getFetchNamespaceDetails(){
    var requestedURL = arguments.length>0?arguments[0]:null;
    $.ajax({
        type: 'GET',
        url: requestedURL,
        dataType: 'text',
        beforeSend: function() {
            $("#loaderDiv1").css("display", "block");
            $('#fetchDetails').html("Processing. Please don't click Refresh!");
        },
        success: function(response) {
            $("#loaderDiv1").css("display", "none");
            console.log("success");
            console.log(response);
            $('#fetchDetails').html(response);
        },
        error: function(response) {
            $("#loaderDiv1").css("display", "none");
            console.log("error");
            console.log(response);
            $('#fetchDetails').html(response);
            $('#fetchDetails').style.color="red";
        }
    });
}

function getSaveNamespaceDetails(){
    var requestedURL = arguments.length>0?arguments[0]:null;
    $.ajax({
        type: 'GET',
        url: requestedURL,
        dataType: 'text',
        beforeSend: function() {
            $("#loaderDiv2").css("display", "block");
            $('#saveDetails').html("Processing. Please don't click Refresh!");
        },
        success: function(response) {
            $("#loaderDiv2").css("display", "none");
            console.log("success");
            console.log(response);
            $('#saveDetails').html(response);
        },
        error: function(response) {
            $("#loaderDiv2").css("display", "none");
            console.log("error");
            console.log(response);
            $('#saveDetails').html(response);
            $('#saveDetails').style.color="red";
        }
    });
}

function getReloadNamespaceDetails(){
    var requestedURL = arguments.length>0?arguments[0]:null;
    $.ajax({
        type: 'GET',
        url: requestedURL,
        dataType: 'text',
        beforeSend: function() {
            $("#loaderDiv3").css("display", "block");
            $('#reloadDetails').html("Processing. Please don't click Refresh!");
        },
        success: function(response) {
            $("#loaderDiv3").css("display", "none");
            console.log("success");
            console.log(response);
            $('#reloadDetails').html(response);
        },
        error: function(response) {
            $("#loaderDiv3").css("display", "none");
            console.log("error");
            console.log(response);
            $('#reloadDetails').html(response);
            $('#reloadDetails').style.color="red";
        }
    });
}

function displayAlert(response) {
    swal({
        title: response.responseText,
        text: "",
        type: "error",
        showCancelButton: true,
        closeOnConfirm: true,
    });
}

function getModalSpecificURL(urlBase){
    var username = getUrlParameter("username");
    var newUrl = urlBase;
    if(username.length > 0){
        var filters = getParameter("filters", newUrl) + ",user:eq:"+username;
        newUrl = removeParam("filters", newUrl);
        newUrl = addParameterToURL("filters", filters, newUrl);
        return newUrl;
    }
    return newUrl;
}

function getClusterName() {
   $.ajax({
       type: 'GET',
       url: "./config?key=fs.defaultFS",
       dataType: 'text',
       success: function(data) {
            $('#connection').html("<h3>Connected to: " + data + ".</h3>");
       }
   });
}

function listIssues() {
    var proxy = getUrlParameter("proxy");
    var urlPath = "./top?limit=1";
    if(proxy.length != 0) {
        urlPath += "&proxy=" + proxy;
    }

    $.ajax({
       type: 'GET',
       url: urlPath,
       dataType: 'json',

       success: function(issues) {
            var html = "<div class='jumbotron' style='background-color: rgba(255, 0, 0, 0.65); padding: 8px'><div class='jumboback'><center><h4><b>Top Issues</b></h4><table style='width:500px'><tr><th>Issue:</th><th>Username/Dir:</th><th>Count/Diskspace:</th></tr>";
            for(var pair in issues) {
                var name = Object.keys(issues[pair])[0];
                if(name != null && name.length > 0) {
                    html += "<tr><td>" + pair + "</td><td>" + Object.keys(issues[pair])[0] + "</td><td>" +
                        nFormatter(Object_values(issues[pair])[0],2) + "</td></tr>";
                }
            }
            html += "</table></center></div></div>";
            $('#top_jumbotron').html(html);
       }
    });
}

function Object_values(obj) {
    return Object.keys(obj).map(e => obj[e])
}

// Redirects to login.html if user is not AUTHENTICATED
function checkIfAuthenticated() {
    var proxy = getUrlParameter("proxy");
    var urlPath = "./credentials";
    if(proxy.length != 0) {
        urlPath += "?proxy=" + proxy;
    }

    $.ajax({
       type: 'GET',
       async: false,
       cache: false,
       url: urlPath,
       dataType: 'text',
       statusCode: {
          401:function() { window.location.replace("./login.html"); }
       }
    });
}

// Shows admin menu in navbar.html if user is ADMIN
function checkIfAdmin() {
    var proxy = getUrlParameter("proxy");
    var urlPath = "./credentials";
    if(proxy.length != 0) {
        urlPath += "?proxy=" + proxy;
    }

    $.ajax({
       type: 'GET',
       url: urlPath,
       dataType: 'text',

       statusCode: {
         400: function(xhr) {
           $("li.admin-menu").hide();
           $("li.user-menu").hide();
           $("li.open-menu").show();
         },
         200: function(xhr) {
           var isAdmin = xhr.includes("ADMIN");
           $("li.open-menu").hide();
           if(isAdmin) {
             $("li.admin-menu").show();
           } else {
             $("li.user-menu").show();
           }
         }
       }
    });
}