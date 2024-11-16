const tags = {
    message_type            : "mtype",
    appId                   : "appId",
    appGroup                : "appGrp",
    heartbeatInterval       : "hbInt",
    heartbeatTimeout        : "hbTo",
    destination_topic       : "destTop",
    component_query_eq      : "eq",
    component_query_results : "cqr",
    app_event_type          : "aet",
    reqId                   : "reqId",
    respId                  : "respId",
    isLastResp              : "lastResp",
    errorDesc               : "ed"
}

const tagValues = {
    message_type : {
        heartbeat                    : "hb",
        registration                 : "reg",
        appUpdate                    : "au",
        component_enquiry            : "ce",
        component_enquiry_response   : "cer",
        component_query              : "cq",
        component_query_response     : "cqe",
        app_event                    : "ae",
        dummy                        : "dummy"
    },

    app_event_type : {
        app_down    :   "app_down"
    },

    errorDesc : {
        not_a_responder : {1 : "not_a_responder"}
    }
}

const topics = {
    heartbeats          : "heartbeats",
    registrations       : "registrations",
    component_query     : "component_query",
    app_events          : "app_events"
}

const appGroups = {
    app_book_keeper : "abk"
}

module.exports.tags       = tags
module.exports.tagValues  = tagValues
module.exports.topics     = topics
module.exports.appGroups  = appGroups