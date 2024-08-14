const tags = {
    message_type        : "mtype",
    appId               : "appId",
    appGroup            : "appGrp",
    heartbeatInterval   : "hbInt",
    heartbeatTimeout    : "hbTo",
    destination_topic   : "destTop"
}

const tagValues = {
    message_type : {
        heartbeat                    : "hb",
        registration                 : "reg",
        appUpdate                    : "au",
        component_enquiry            : "ce",
        component_enquiry_response   : "cer"
    }
}

const topics = {
    heartbeats      : "heartbeats",
    registrations   : "registrations"
}

module.exports.tags       = tags
module.exports.tagValues  = tagValues
module.exports.topics  = topics