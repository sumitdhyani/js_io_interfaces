const tags = {
    message_type        : "message_type",
    appId               : "appId",
    appGroup            : "appGrp",
    heartbeatInterval   : "heartbeatInterval",
    heartbeatTimeout    : "heartbeatTimeout"
}

const tagValues = {
    message_type : {
        heartbeat       : "heartbeat",
        registration    : "registration",
        appUpdate       : "appUpdate"
    }
}

const topics = {
    heartbeats      : "heartbeats",
    registrations   : "registrations"
}

module.exports.tags       = tags
module.exports.tagValues  = tagValues
module.exports.topics  = topics